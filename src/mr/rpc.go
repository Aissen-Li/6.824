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
// Worker --register--> Master --> record the Worker --register ok --> Worker
type RegisterArgs struct {
}

type RegisterResult struct {
	WorkerId int
}

// Worker --reuqireTask --> Master --> generate a Task -- task --> Worker
type RequireArgs struct {
	WorkerId int
}

type RequireResult struct {
	Task *Task
}

// Worker --reportTask--> Master
// With info of is the task done, the task seq, which type is the task and which worker finishes this task
type ReportArgs struct {
	Done     bool
	Seq      int
	WorkerId int
	Type     TaskType
}

type ReportResult struct {
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
