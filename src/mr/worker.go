package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

const WorkerProductDir = "product"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func formatFname(filename string) string {
	return fmt.Sprintf("%s/%s", WorkerProductDir, filename)
}

func readFile(filename string) string {
	// open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	// read content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func storeKVToJSONFile(filename string, kvs []KeyValue) bool {
	// open file
	filename = formatFname(filename)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	defer file.Close()
	// encode kvs to json file
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("failed encode kv:", kv)
			return false
		}
	}
	return true
}

func readKVFromJSONFile(filename string) []KeyValue {
	// open file
	filename = formatFname(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	// decode kvs from json file
	dec := json.NewDecoder(file)
	kvs := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kvs = append(kvs, kv)
	}
	return kvs
}

func mapWorker(t *TaskData, mapf func(string, string) []KeyValue) {
	t.State = Running
	fmt.Println("MapWorker task:", *t)
	// read & map input files
	ifilemap := make(map[string][]KeyValue)
	for _, fn := range t.Filenames {
		content := readFile(fn)
		kvs := mapf(fn, content)
		for _, kv := range kvs {
			rn := ihash(kv.Key) % t.NReduce
			ifn := fmt.Sprintf("mr-inter-%s-%d", t.ID, rn)
			ifilemap[ifn] = append(ifilemap[ifn], kv)
		}
	}

	// store to intermediate files
	ifilenames := []string{}
	for ifn, kvs := range ifilemap {
		ifilenames = append(ifilenames, ifn)
		storeKVToJSONFile(ifn, kvs)
	}

	// notify the master
	args := FinishTaskArgs{t, ifilenames}
	reply := &FinishTaskReply{}
	success := call("Master.FinishTask", &args, &reply)

	if !success {
		log.Fatal("call Master.FinishTask failed")
	}
	if reply.Status != OK {
		log.Fatal("finish map task w/ status:", reply.Status)
	}
}

func reduceWorker(t *TaskData, nthReduce int, reducef func(string, []string) string) {
	t.State = Running
	fmt.Println("ReduceWorker task:", *t)
	// read kvs from intermediate files & group by key
	kvmap := make(map[string][]string)
	for _, fn := range t.Filenames {
		kvs := readKVFromJSONFile(fn)
		for _, kv := range kvs {
			kvmap[kv.Key] = append(kvmap[kv.Key], kv.Value)
		}
	}

	// call reducef for each key & write to output files
	outfn := formatFname(fmt.Sprintf("mr-out-%d", nthReduce))
	outf, _ := os.Create(outfn)
	defer outf.Close()
	for key, values := range kvmap {
		val := reducef(key, values)
		fmt.Fprintf(outf, "%v %v\n", key, val)
	}

	// notify the master
	args := FinishTaskArgs{t, []string{outfn}}
	reply := &FinishTaskReply{}
	success := call("Master.FinishTask", &args, &reply)

	if !success {
		log.Fatal("call Master.FinishTask failed")
	}
	if reply.Status != OK {
		log.Fatal("finish reduce task w/ status:", reply.Status)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	os.RemoveAll(WorkerProductDir)
	os.Mkdir(WorkerProductDir, os.ModeAppend)

	// Your worker implementation here.
	workerCount := 0
	for {
		workerID := fmt.Sprint(workerCount)
		workerCount++
		args := GetTaskArgs{ID: workerID}
		reply := GetTaskReply{}
		success := call("Master.GetTask", &args, &reply)

		// call failed
		if !success {
			continue
		}

		// all tasks finished
		if reply.Status == AllDone {
			break
		}

		task := reply.Task
		// something wrong
		if reply.Status != OK || task == nil {
			continue
		}

		// start a map worker task
		if task.Type == MapTask {
			go mapWorker(task, mapf)
		} else { // start a reduce worker task
			go reduceWorker(task, reply.NthReduce, reducef)
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
