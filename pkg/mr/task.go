package mr

type TaskState int

const (
	IDLE TaskState = iota
	IN_PROGRESS
	COMPLETED
)

type TaskType int

const (
	EXIT TaskType = iota
	WAIT
	MAP
	REDUCE
)

type Task struct {
	TaskId    int
	TaskState TaskState
	TaskType  TaskType
	NReduce   uint8
	NMap      uint8
}

// Error implements [error].
func (t Task) Error() string {
	panic("something went not as expected")
}

type RequestTask struct {
	WorkerId int
}
