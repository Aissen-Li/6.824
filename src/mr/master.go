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

//some useful status number
const (
	TaksStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusError   = 4
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type Master struct {
	// Your definitions here.
	files     []string
	nReduce   int
	taskType  TaskType
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerNum int
	taskCh    chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// if a Worker calls to register, Master record the current workerNum and sends back a WorkerId
func (m *Master) RegisterWorker(args *RegisterArgs, result *RegisterResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerNum += 1
	result.WorkerId = m.workerNum
	return nil
}

// if a Worker calls to require a task, Master generate a task and sends back
func (m *Master) GenerateTask(args *RequireArgs, result *RequireResult) error {
	task := <-m.taskCh
	result.Task = &task

	if task.Active {
		m.RegisterTask(args, &task)
	}
	return nil
}

// When a Worker requires a task, Master should register this task
func (m *Master) RegisterTask(args *RequireArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Type != m.taskType {
		panic("Task Type not matches, please check again")
	}

	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

// When a Worker reports a task, Master should update realated info
func (m *Master) GetReportTask(args *ReportArgs, result *ReportResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.taskType != args.Type || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}
	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusError
	}
	go m.schedule()
	return nil
}

// Master schedule all the Map and Reduce tasks
func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		return
	}

	allTasksFinished := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaksStatusReady:
			allTasksFinished = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allTasksFinished = false
		case TaskStatusRunning:
			allTasksFinished = false
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusError:
			allTasksFinished = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			panic("Error in task status")
		}
	}
	if allTasksFinished {
		if m.taskType == MapType {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

// Master get a task by task index
func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		Filename: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		Seq:      taskSeq,
		Type:     m.taskType,
		Active:   true,
	}
	if task.Type == MapType {
		task.Filename = m.files[taskSeq]
	}
	return task
}

// Master initial all the map tasks during master init
func (m *Master) initMapTask() {
	m.taskType = MapType
	m.taskStats = make([]TaskStat, len(m.files))
}

// Master initial all the reduce tasks after all map tasks are done
func (m *Master) initReduceTask() {
	m.taskType = ReduceType
	m.taskStats = make([]TaskStat, m.nReduce)
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
	// ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

// Master time management, every few seconds to check all the tasks status
func (m *Master) timeSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}
	m.initMapTask()
	go m.timeSchedule()
	m.server()
	return &m
}
