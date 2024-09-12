package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

// EHR represents an individual health record
type EHR struct {
	PatientID string
	Name      string
	Age       string
	Diagnosis string
	Treatment string
}

// MapReduce structure
type MapReduce struct {
	Files   []string
	NMap    int
	NReduce int
}

// Master structure
type Master struct {
	mr          *MapReduce
	mapTasks    chan int
	reduceTasks chan int
	done        chan bool
}

// ParseEHR function to parse a line of EHR data
func ParseEHR(line string) EHR {
	fields := strings.Fields(line)
	return EHR{
		PatientID: fields[0],
		Name:      fields[1] + " " + fields[2],
		Age:       fields[3],
		Diagnosis: fields[4],
		Treatment: fields[5],
	}
}

// MapTask function
func MapTask(filename string, task int, mr *MapReduce, wg *sync.WaitGroup, results chan<- error) {
	defer wg.Done()
	diagnosisCounts := make(map[string]int)
	treatmentCounts := make(map[string]int)
	file, err := os.Open(filename)
	if err != nil {
		results <- err
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ehr := ParseEHR(scanner.Text())
		diagnosisCounts[ehr.Diagnosis]++
		treatmentCounts[ehr.Treatment]++
	}

	if err := scanner.Err(); err != nil {
		results <- err
		return
	}

	diagnosisFile, err := os.Create(fmt.Sprintf("map-diagnosis-%s-%d.txt", filename, task))
	if err != nil {
		results <- err
		return
	}
	defer diagnosisFile.Close()

	for diagnosis, count := range diagnosisCounts {
		fmt.Fprintf(diagnosisFile, "%v %v\n", diagnosis, count)
	}

	treatmentFile, err := os.Create(fmt.Sprintf("map-treatment-%s-%d.txt", filename, task))
	if err != nil {
		results <- err
		return
	}
	defer treatmentFile.Close()

	for treatment, count := range treatmentCounts {
		fmt.Fprintf(treatmentFile, "%v %v\n", treatment, count)
	}

	results <- nil
}

// ReduceTask function
func ReduceTask(task int, mr *MapReduce, wg *sync.WaitGroup, results chan<- error) {
	defer wg.Done()
	diagnosisCounts := make(map[string]int)
	treatmentCounts := make(map[string]int)
	for i := 0; i < mr.NMap; i++ {
		diagnosisFilename := fmt.Sprintf("map-diagnosis-%s-%d.txt", mr.Files[i], i)
		diagnosisFile, err := os.Open(diagnosisFilename)
		if err != nil {
			results <- err
			return
		}
		defer diagnosisFile.Close()

		scanner := bufio.NewScanner(diagnosisFile)
		for scanner.Scan() {
			var diagnosis string
			var count int
			fmt.Sscanf(scanner.Text(), "%v %v", &diagnosis, &count)
			diagnosisCounts[diagnosis] += count
		}
		if err := scanner.Err(); err != nil {
			results <- err
			return
		}

		treatmentFilename := fmt.Sprintf("map-treatment-%s-%d.txt", mr.Files[i], i)
		treatmentFile, err := os.Open(treatmentFilename)
		if err != nil {
			results <- err
			return
		}
		defer treatmentFile.Close()

		scanner = bufio.NewScanner(treatmentFile)
		for scanner.Scan() {
			var treatment string
			var count int
			fmt.Sscanf(scanner.Text(), "%v %v", &treatment, &count)
			treatmentCounts[treatment] += count
		}
		if err := scanner.Err(); err != nil {
			results <- err
			return
		}
	}

	outputFile, err := os.Create("reduce-out.txt")
	if err != nil {
		results <- err
		return
	}
	defer outputFile.Close()

	fmt.Fprintln(outputFile, "Diagnosis Counts:")
	for diagnosis, count := range diagnosisCounts {
		fmt.Fprintf(outputFile, "%v %v\n", diagnosis, count)
	}

	fmt.Fprintln(outputFile, "Treatment Counts:")
	for treatment, count := range treatmentCounts {
		fmt.Fprintf(outputFile, "%v %v\n", treatment, count)
	}
	results <- nil
}

// NewMaster function
func NewMaster(mr *MapReduce) *Master {
	mapTasks := make(chan int, mr.NMap)
	reduceTasks := make(chan int, mr.NReduce)
	for i := 0; i < mr.NMap; i++ {
		mapTasks <- i
	}
	for i := 0; i < mr.NReduce; i++ {
		reduceTasks <- i
	}
	return &Master{mr: mr, mapTasks: mapTasks, reduceTasks: reduceTasks, done: make(chan bool)}
}

// AssignMapTask function
func (m *Master) AssignMapTask(args int, reply *int) error {
	select {
	case task := <-m.mapTasks:
		*reply = task
		return nil
	default:
		return fmt.Errorf("no more map tasks")
	}
}

// AssignReduceTask function
func (m *Master) AssignReduceTask(args int, reply *int) error {
	select {
	case task := <-m.reduceTasks:
		*reply = task
		return nil
	default:
		return fmt.Errorf("no more reduce tasks")
	}
}

// Done function
func (m *Master) Done(args int, reply *string) error {
	m.done <- true
	*reply = "All tasks are done"
	return nil
}

// Wait function
func (m *Master) Wait() {
	<-m.done
}

func main() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	var filenames []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".txt") {
			filenames = append(filenames, file.Name())
		}
	}
	nMap := len(filenames)
	nReduce := 1 // Change to 1

	mr := &MapReduce{
		Files:   filenames,
		NMap:    nMap,
		NReduce: nReduce,
	}

	master := NewMaster(mr)

	rpc.Register(master)
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("Listener error:", err)
	}
	defer listener.Close()
	go rpc.Accept(listener)

	var wg sync.WaitGroup
	mapResults := make(chan error, nMap)
	reduceResults := make(chan error, nReduce)

	// Concurrent execution of map tasks
	for i, filename := range filenames {
		wg.Add(1)
		go MapTask(filename, i, mr, &wg, mapResults)
	}
	wg.Wait()
	close(mapResults)

	// Check map task results
	for err := range mapResults {
		if err != nil {
			log.Fatal("Map task error:", err)
		}
	}

	// Concurrent execution of reduce tasks
	for i := 0; i < nReduce; i++ {
		wg.Add(1)
		go ReduceTask(i, mr, &wg, reduceResults)
	}
	wg.Wait()
	close(reduceResults)

	// Check reduce task results
	for err := range reduceResults {
		if err != nil {
			log.Fatal("Reduce task error:", err)
		}
	}

	var doneReply string
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatal("Dialing error:", err)
	}
	err = client.Call("Master.Done", 0, &doneReply)
	if err != nil {
		log.Fatal("Done error:", err)
	}
	fmt.Println(doneReply)
	master.Wait()
}
