package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Files     []string
	Tasks     [2][]Task
	NReduce   int
	NMap      int
	TaskState []int
	State     int
	time      time.Time
	Mu        sync.Mutex
	Wg        sync.WaitGroup
}

type Task struct {
	ID int
	//State    int    // 0:idle 1:in-progress 2:completed
	TaskType int    // 0:map 1:reduce
	FileName string // map: input file name
	Time     time.Time
	//RFileNames []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	//log.Println("RequestTask executing...")
	c.Mu.Lock()
	defer c.Mu.Unlock()

	var taskType int

	if c.State != 2 {
		taskType = c.State
	} else {
		fmt.Println(reply.Done)
		reply.Done = true
		return nil
	}

	if len(c.Tasks[taskType]) == 0 {
		reply.Task.ID = -1
		//log.Println("there is no tasks")
		return nil
	}

	index := 0
	for index < len(c.Tasks[taskType]) && c.TaskState[index] != 0 {
		index++
	}
	if index == len(c.Tasks[taskType]) {
		reply.Task.ID = -1
		//log.Println("all tasks are busy")
		return nil
	}

	// assign a task
	c.TaskState[index] = 1

	// watching task
	go func(i int) {
		for {
			c.Mu.Lock()
			task := c.Tasks[taskType][i]
			state := c.TaskState[i]
			c.Mu.Unlock()
			switch {
			case state == 2:
				//fmt.Printf("type: %d id: %d done!\n", taskType, i)
				c.Wg.Done()
				return
			case task.Time.Add(10 * time.Second).Before(time.Now()):
				c.Mu.Lock()
				c.TaskState[i] = 0
				c.Mu.Unlock()
				return
			default:
				time.Sleep(time.Millisecond * 100)
			}
		}
	}(index)

	reply.NReduce = c.NReduce
	reply.NMap = c.NMap
	reply.Task = c.Tasks[taskType][index]
	c.Tasks[taskType][index].Time = time.Now()

	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {

	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.TaskState[args.Task.ID] = 2
	//log.Printf("task %d done", args.Task.ID)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	// Your code here.
	c.Mu.Lock()
	if c.State == 2 {
		ret = true
		fmt.Printf("test finished, time: %s\n", time.Now().Sub(c.time).String())
	}
	c.Mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	//fmt.Println("MakeCoordinator executing...")
	c := Coordinator{}
	c.time = time.Now()
	// create dir
	//for i := 0; i < nReduce; i++ {
	//	dirName := "map-worker-" + strconv.Itoa(i)
	//	_ = os.Mkdir(dirName, os.ModePerm)
	//	//if err != nil {
	//	//	log.Fatalf("cannot create %v", dirName)
	//	//}
	//}

	// Your code here.
	c.Files = files
	c.NReduce = nReduce
	c.NMap = len(files)
	c.State = 0
	c.TaskState = make([]int, c.NMap)

	c.Wg.Add(c.NMap)

	go func(nReduce int) {
		// wait map tasks done
		c.Wg.Wait()
		c.Mu.Lock()
		c.State = 1
		c.TaskState = make([]int, nReduce)
		c.Mu.Unlock()
		c.Wg.Add(nReduce)
		for i := 0; i < nReduce; i++ {
			task := Task{
				ID:       i,
				TaskType: 1,
			}
			c.Mu.Lock()
			c.Tasks[1] = append(c.Tasks[1], task)
			c.Mu.Unlock()
		}
		// wait reduce tasks done
		c.Wg.Wait()
		c.Mu.Lock()
		c.State = 2
		c.Mu.Unlock()
	}(nReduce)

	// initial map task
	for i, file := range files {
		c.Tasks[0] = append(c.Tasks[0], Task{
			ID:       i,
			TaskType: 0,
			FileName: file,
		})
	}

	c.server()
	return &c
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}
