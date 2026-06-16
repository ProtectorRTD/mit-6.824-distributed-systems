package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//TODO read about the wait in MapReduce
//Intermidiate file write Worker or Master ?

type Coordinator struct {
	mutex       sync.Mutex
	NReduce     int
	NMap        int
	Files       []string
	TaskQueue   []Task
	pendingTask uint8
}

func (c *Coordinator) RequestTask(args *RequestTask, reply *Task) error {
	c.mutex.Lock()
	head := c.TaskQueue[0]
	//3 == REDUCE
	if head.TaskType == 3 {
		c.pendingTask--
	}
	*reply = head
	c.TaskQueue = c.TaskQueue[1:]
	c.mutex.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.pendingTask == 0 {
		return true
	} else {
		return false
	}
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(fileNames []string, nReduce int) *Coordinator {
	mapAmount := len(fileNames)
	c := &Coordinator{
		mutex:       sync.Mutex{},
		NReduce:     nReduce,
		NMap:        mapAmount,
		Files:       fileNames,
		TaskQueue:   make([]Task, 0),
		pendingTask: uint8(mapAmount),
	}
	for i := 0; i < mapAmount; i++ {
		task := Task{
			TaskId:    1,
			TaskState: IDLE,
			TaskType:  MAP,
			NReduce:   uint8(nReduce),
			NMap:      uint8(mapAmount),
			FileName:  fileNames[i],
		}
		c.TaskQueue = append(c.TaskQueue, task)
	}
	c.server()
	return c
}
