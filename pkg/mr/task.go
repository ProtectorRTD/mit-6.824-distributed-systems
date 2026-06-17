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

// in future adjust, what is public/private
type Task struct {
	TaskId    string
	TaskState TaskState
	TaskType  TaskType
	NReduce   uint8
	NMap      uint8
	FileName  string
}

// Error implements [error].
func (t Task) Error() string {
	panic("something went not as expected")
}

type RequestTask struct {
	WorkerId int
	Result   ResultOfTask
}

type ResultOfTask struct {
	TaskId  string
	Success bool
}
