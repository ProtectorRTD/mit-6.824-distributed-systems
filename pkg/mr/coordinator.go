package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// map[int] -> where int workerId (for now, will be change)
type Coordinator struct {
	NReduce        int
	NMap           int
	Files          []string
	TaskQueue      []Task
	TaskProcessing map[int]Task
}

func (c *Coordinator) RequestTask(args *RequestTask, reply *Task) error {

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
	//podumat -> remainingMapTasks == 0 && remainingReduceTasks == 0
	if len(c.TaskQueue) == 0 && len(c.TaskProcessing) == 0 {
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		NReduce:        nReduce,
		NMap:           len(files),
		Files:          files,
		TaskQueue:      make([]Task, 0),
		TaskProcessing: make(map[int]Task),
	}

	c.server()
	return c
}
