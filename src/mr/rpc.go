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

const (
	TASK_MAP    = "map"
	TASK_REDUCE = "reduce"
)

type MapTask struct {
	Filename     string
	MapIndex     int //Index of the task
	ReduceNumber int //nReduce
}

type ReduceTask struct {
	ReduceIndex int
	MapNumber   int
}

type Task struct {
	Type       string
	MapTask    MapTask
	ReduceTask ReduceTask
}

// Add your RPC definitions here.
type ApplyForTaskArgs struct {
	CompletedTask Task
}

type ApplyForTaskReply struct {
	Done bool
	Task Task
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
