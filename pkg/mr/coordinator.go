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
)

//TODO read about the wait in MapReduce

type Coordinator struct {
	mutex        sync.Mutex
	NReduce      int
	NMap         int
	Files        []string
	Tasks        []Task
	pendingTask  map[string]Task
	RemainingMap uint8
}

//http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html
//нельзя давать редьюс таск, пока не сделан маппинг
//есть кейс, когда воркер сделает таск, но маппинг ещё не придет,
//второй момент, мастер может дать таску, но если за 10 сек не пришел ответ
//то мы возвращаем таску в пул и все (и считаем что эта таска не была совершена)

func (c *Coordinator) RequestTask(args *RequestTask, reply *Task) error {
	c.mutex.Lock()
	//invariant, что нету мапТасков + все сделаны
	if c.RemainingMap <= 0 {
		c.generateReduceTask()
	}

	//TODO если реквест, ассинхронный таймер на 10 сек, что все ок, если прошло,
	//то возвращаем таску в очередь тип воркер мертв
	head := c.Tasks[0]
	*reply = head
	c.Tasks = c.Tasks[1:]
	c.mutex.Unlock()
	return nil
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
	if !result.Success {
		return fmt.Errorf("worker reported failure for task %s", result.TaskId)
	}
	taskId := result.TaskId
	if c.pendingTask[taskId].TaskType == 2 {
		c.RemainingMap--
	}
	delete(c.pendingTask, taskId)
	c.mutex.Unlock()
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

// main/mrcoordinator.go calls this function.
func MakeCoordinator(fileNames []string, nReduce int) *Coordinator {
	mapAmount := len(fileNames)
	c := &Coordinator{
		mutex:        sync.Mutex{},
		NReduce:      nReduce,
		NMap:         mapAmount,
		Files:        fileNames,
		Tasks:        make([]Task, 0),
		pendingTask:  make(map[string]Task),
		RemainingMap: uint8(mapAmount),
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
