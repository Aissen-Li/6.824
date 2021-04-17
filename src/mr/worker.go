package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	Id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.runProcess()
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//Worker register and get a WorkerId from Master
func (w *worker) register() {
	args := &RegisterArgs{}
	result := &RegisterResult{}
	if registerReply := call("Master.RegisterWorker", args, result); !registerReply {
		log.Fatal("Register fail")
	}
	w.Id = result.WorkerId
}

//Worker require a Task from Master
func (w *worker) requireTask() Task {
	args := RequireArgs{}
	args.WorkerId = w.Id
	result := RequireResult{}
	if requireReply := call("Master.GenerateTask", &args, &result); !requireReply {
		os.Exit(1)
	}
	return *result.Task
}

//Worker report a Task to Master
func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Type = t.Type
	args.WorkerId = w.Id
	result := ReportResult{}
	if reply := call("Master.GetReportTask", &args, &result); !reply {
		return
	}
}

//Worker runProcess
func (w *worker) runProcess() {
	for {
		t := w.requireTask()
		if !t.Active {
			return
		}
		w.doTask(t)
	}
}

//Worker do the task, map or reduce task
func (w *worker) doTask(t Task) {
	switch t.Type {
	case MapType:
		w.doMapTask(t)
	case ReduceType:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("Wrong task type :%v", t.Type))
	}
}

//Worker do the mapTask
func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	kva := w.mapf(t.Filename, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := reduceName(t.Seq, idx)
		file, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := file.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

//Worker do the reduceTask
func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMap; idx++ {
		fileName := reduceName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)

		}
	}

	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample()

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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
