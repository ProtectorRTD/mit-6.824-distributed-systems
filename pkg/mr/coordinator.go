package mr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//TODO read about the wait in MapReduce

type Coordinator struct {
	mutex           sync.Mutex
	NReduce         int
	NMap            int
	Files           []string
	Tasks           []Task
	pendingTask     map[string]Task
	startedAt       map[string]time.Time
	RemainingMap    uint8
	RemainingReduce uint8
	reduceGenerated bool
}

// стейты плохо менеджерится тасков, надо исправить
// http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html
func (c *Coordinator) RequestTask(args *RequestTask, reply *Task) error {
	c.mutex.Lock()
	//invariant, что нету мапТасков + все сделаны
	// (бесконечно может быть), попросит новую, создат редьюс
	if c.RemainingReduce <= 0 && c.RemainingMap <= 0 {
		*reply = *c.generateExitTask()
		c.Done()
		return nil
	}
	if c.RemainingMap <= 0 && c.reduceGenerated == false {
		c.reduceGenerated = true
		c.generateReduceTask()
	}

	if len(c.Tasks) == 0 {
		c.mutex.Unlock()
		return nil
	}

	head := c.Tasks[0]
	c.Tasks = c.Tasks[1:]

	*reply = head

	taskId := head.TaskId
	c.pendingTask[taskId] = head
	c.startedAt[taskId] = time.Now()
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) generateExitTask() *Task {
	task := &Task{
		TaskId:    newID(),
		TaskState: COMPLETED,
		TaskType:  EXIT,
		NReduce:   uint8(c.NReduce),
		NMap:      uint8(c.NMap),
	}
	return task
}

func (c *Coordinator) generateReduceTask() {
	for i := 0; i < c.NReduce; i++ {
		task := Task{
			TaskId:    newID(),
			TaskState: IDLE,
			TaskType:  REDUCE,
			NReduce:   uint8(c.NReduce),
			NMap:      uint8(c.NMap),
		}
		c.Tasks = append(c.Tasks, task)
	}
}

func (c *Coordinator) reportDone(result *ResultOfTask) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !result.Success {
		return fmt.Errorf("worker reported failure for task %s", result.TaskId)
	}
	taskId := result.TaskId
	task, ok := c.pendingTask[taskId]
	if !ok {
		// taskId не существует
	} else {
		if c.pendingTask[taskId].TaskType == MAP {

			c.RemainingMap--
		}
		if c.pendingTask[taskId].TaskType == REDUCE {
			c.RemainingReduce--
		}
		delete(c.pendingTask, taskId)
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.RemainingMap > 0 {
		return false
	}

	if len(c.pendingTask) > 0 {
		return false
	}

	if len(c.Tasks) == 0 {
		return true
	}

	return false
}

func (c *Coordinator) sheduler() {
	for {
		time.Sleep(time.Second)
		c.mutex.Lock()
		for taskId, task := range c.pendingTask {
			started := c.startedAt[taskId]

			if time.Since(started) > 10*time.Second {
				task.TaskState = IDLE

				c.Tasks = append(c.Tasks, task)

				delete(c.pendingTask, taskId)
				delete(c.startedAt, taskId)
			}
		}
		c.mutex.Unlock()
	}
}

// main/mrcoordinator.go calls this function.
func MakeCoordinator(fileNames []string, nReduce int) *Coordinator {
	mapAmount := len(fileNames)
	c := &Coordinator{
		mutex:           sync.Mutex{},
		NReduce:         nReduce,
		NMap:            mapAmount,
		Files:           fileNames,
		Tasks:           make([]Task, 0),
		pendingTask:     make(map[string]Task),
		RemainingMap:    uint8(mapAmount),
		RemainingReduce: uint8(nReduce),
		reduceGenerated: false,
	}
	for i := 0; i < mapAmount; i++ {
		task := Task{
			TaskId:    newID(),
			TaskState: IDLE,
			TaskType:  MAP,
			NReduce:   uint8(nReduce),
			NMap:      uint8(mapAmount),
			FileName:  fileNames[i],
		}
		c.Tasks = append(c.Tasks, task)
	}
	c.server()
	go c.sheduler()
	return c
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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
