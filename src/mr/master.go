package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type SafeList[T comparable] struct {
	m    sync.Mutex
	list []T
}

func (sf *SafeList[_]) length() int {
	return len(sf.list)
}

func (sf *SafeList[T]) pop() T {
	return sf.removeAt(sf.length() - 1)
}

func (sf *SafeList[T]) removeAt(index int) T {
	sf.m.Lock()
	defer sf.m.Unlock()
	if index < 0 || index > sf.length() {
		return *new(T)
	}
	elem := sf.list[index]
	sf.list = append(sf.list[:index], sf.list[index+1:]...)
	return elem
}

func (sf *SafeList[T]) append(elem T) {
	sf.m.Lock()
	defer sf.m.Unlock()
	sf.list = append(sf.list, elem)
}

func (sf *SafeList[T]) all(allf func(T) bool) bool {
	for _, el := range sf.list {
		if !allf(el) {
			return false
		}
	}
	return true
}

type Master struct {
	// Your definitions here.
	mutex                sync.Mutex
	NReduce              int
	FileList             SafeList[string]
	MapTaskList          SafeList[*TaskData]
	IntermediateFileList SafeList[string]
	ReduceTaskList       SafeList[*TaskData]
	OutputList           SafeList[string]
	cancelledWorkers     []string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) cancelTimeoutWorkers() {
	tasks := append(m.MapTaskList.list, m.ReduceTaskList.list...)
	for _, mt := range tasks {
		if mt.State != Finished {
			mt.CountDown--
			if mt.CountDown <= 0 {
				// cancel task worker
				m.cancelledWorkers = append(m.cancelledWorkers, mt.ID)
			}
		}
	}
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// validate args
	if args == nil || args.ID == "" {
		reply.Status = BadRequest
		return nil
	}

	// get filename
	filename := m.FileList.pop()
	taskType := MapTask
	if filename == "" { // getting map tasks
		taskType = ReduceTask
		filename = m.IntermediateFileList.pop()
	}

	if filename == "" { // no more tasks to assign
		if m.Done() {
			reply.Status = AllDone
		} else {
			reply.Status = Waiting
		}
		return nil
	}

	// init a task
	task := &TaskData{args.ID, Pending, taskType, 10, m.NReduce, filename}
	// record the task
	if taskType == MapTask {
		m.MapTaskList.append(task)
	} else {
		m.ReduceTaskList.append(task)
	}
	// set reply
	reply.Status = OK
	reply.Task = task

	// sleep 1 second
	time.Sleep(time.Second)
	// cancel timeout workers
	m.cancelTimeoutWorkers()

	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	// validate args
	if args == nil || args.Task == nil {
		reply.Status = BadRequest
		return nil
	}
	// filter out cancelled workers
	for _, workerID := range m.cancelledWorkers {
		if workerID == args.Task.ID {
			reply.Status = Cancelled
			return nil
		}
	}

	// append output file
	if args.Task.Type == MapTask {
		m.IntermediateFileList.append(args.OutputFilename)
	} else {
		m.OutputList.append(args.OutputFilename)
	}
	// mark task as finished
	args.Task.State = Finished
	// set reply
	reply.Status = OK

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Println("server: rpc")
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	taskDone := func(t *TaskData) bool { return t.State == Finished }
	return m.FileList.length() == 0 &&
		m.MapTaskList.length() > 0 &&
		m.MapTaskList.all(taskDone) &&
		m.ReduceTaskList.length() > 0 &&
		m.ReduceTaskList.all(taskDone)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NReduce = nReduce
	m.FileList = SafeList[string]{list: files}

	m.MapTaskList = SafeList[*TaskData]{}
	m.IntermediateFileList = SafeList[string]{}

	m.ReduceTaskList = SafeList[*TaskData]{}
	m.OutputList = SafeList[string]{}

	m.cancelledWorkers = []string{}

	m.server()
	return &m
}
