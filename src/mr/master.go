package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
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

func (sf *SafeList[T]) append(elem ...T) {
	sf.m.Lock()
	defer sf.m.Unlock()
	sf.list = append(sf.list, elem...)
}

func (sf *SafeList[T]) all(allf func(T) bool) bool {
	for _, el := range sf.list {
		if !allf(el) {
			return false
		}
	}
	return true
}

func (sf *SafeList[T]) filter(filterf func(T) bool) []T {
	slice := []T{}
	for _, el := range sf.list {
		if filterf(el) {
			slice = append(slice, el)
		}
	}
	return slice
}

type Master struct {
	// Your definitions here.
	mutex                sync.Mutex
	NReduce              int
	CurrReduceNum        int
	FileList             SafeList[string]
	MapTaskList          SafeList[*TaskData]
	IntermediateFileList SafeList[string]
	ReduceTaskList       SafeList[*TaskData]
	OutputList           SafeList[string]
}

// Your code here -- RPC handlers for the worker to call.

func isTaskDone(t *TaskData) bool {
	return t.State == Finished
}

func (m *Master) cancelTimeoutWorkers() {
	tasks := append(m.MapTaskList.list, m.ReduceTaskList.list...)
	for _, t := range tasks {
		if t.State != Finished {
			t.CountDown--
			if t.CountDown <= 0 {
				// cancel task worker
				t.isCancelled = true
				if t.Type == MapTask {
					m.FileList.append(t.Filenames...)
				} else {
					m.IntermediateFileList.append(t.Filenames...)
				}
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
	files := []string{}
	taskType := MapTask
	if filename := m.FileList.pop(); filename == "" { // is reduce tasks
		taskType = ReduceTask
		// find all intermediate files of nth reduce
		files = m.IntermediateFileList.filter(func(fn string) bool {
			return strings.HasSuffix(fn, fmt.Sprintf("-%d", m.CurrReduceNum))
		})
		reply.NthReduce = m.CurrReduceNum
		m.CurrReduceNum++
	} else {
		files = append(files, filename)
	}

	if len(files) == 0 { // no tasks to assign
		if m.Done() {
			reply.Status = AllDone
		} else {
			reply.Status = Waiting
		}
		return nil
	}

	// init a task
	task := &TaskData{args.ID, Pending, taskType, 10, m.NReduce, files, false}
	// record the task
	if taskType == MapTask {
		m.MapTaskList.append(task)
	} else {
		m.ReduceTaskList.append(task)
	}
	// set reply
	reply.Status = OK
	reply.Task = task

	// assign a task every 1 second
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
	if args.Task.isCancelled {
		reply.Status = Cancelled
		return nil
	}

	// append output file
	if args.Task.Type == MapTask {
		m.IntermediateFileList.append(args.OutputFiles...)
	} else { // args.Task.Type == ReduceTask
		m.OutputList.append(args.OutputFiles...)
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
	return m.FileList.length() == 0 &&
		m.MapTaskList.length() > 0 &&
		m.MapTaskList.all(isTaskDone) &&
		m.ReduceTaskList.length() > 0 &&
		m.ReduceTaskList.all(isTaskDone)
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

	m.server()
	return &m
}
