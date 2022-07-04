package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskState int64

const (
	Pending TaskState = iota
	InProgress
	Finished
)

type TaskType int64

const (
	MapTask TaskType = iota
	ReduceTask
)

type TaskData struct {
	ID          string
	State       TaskState
	Type        TaskType
	CountDown   int
	NReduce     int
	Filenames   []string
	isCancelled bool
}

// func (t MapTask) Compare(other MapTask) int {
// 	return t.ID.Compare(other.ID)
// }

// Args & Replys

type ReplyStatus string

const (
	OK         = "OK"
	BadRequest = "BadRequest"
	Waiting    = "Waiting"
	Cancelled  = "Cancelled"
	AllDone    = "AllDone"
)

type GetTaskArgs struct {
	ID string
}

type GetTaskReply struct {
	Status    ReplyStatus
	Task      *TaskData
	NthReduce int
}

type FinishTaskArgs struct {
	Task        *TaskData
	OutputFiles []string
}

type FinishTaskReply struct {
	Status ReplyStatus
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
