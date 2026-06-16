package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

//TODO
//ihash прочитать бакет мап редьюс (визуал диаграмму)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	var task = requestTask()
	for {
		switch task.TaskType {
		case 0:
			//exit -> stop the endless loop
			return
		case 1:
			doWait(task)
		case 2:
			doMap(task, mapf)
		case 3:
			doReduce(task)
		}
	}
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	contentBytes, err := os.ReadFile(task.FileName)
	if err != nil {
		// handle error
		print("File not read correctly at Worker doMap")
	}
	kva := mapf(task.FileName, string(contentBytes))
}

func doReduce(task Task) {
	//reduce logic
}

func doWait(task Task) {
	//wait logic
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func requestTask() Task {
	// declare an argument structure.
	args := RequestTask{}
	// declare a reply structure.
	reply := Task{}
	// send the RPC request, wait for the reply.
	call("Coordinator.RequestTask", &args, &reply)
	return reply
}

// not need to change
// =================================================================
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
