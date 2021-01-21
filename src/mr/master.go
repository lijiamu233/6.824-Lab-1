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

type Master struct {
	// Your definitions here.
	files []string
	//Mutex Lock
	Mu   sync.Mutex
	Cond *sync.Cond

	//num of map and reduce
	mMap    int
	nReduce int

	//num of completed
	completedMap    int
	completedReduce int

	//info of running tasks
	runningMap    map[int]int64
	runningReduce map[int]int64

	//chans
	mapChan    chan int
	reduceChan chan int
}

const (
	GIVE_TASK_SUCCESS = "success"
	GIVE_TASK_DONE    = "done"
	GIVE_TASK_FAIL    = "fail"
)

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	m.Cond.L.Lock()
	defer m.Cond.L.Unlock()

	//if a task was given but not finish, do it
	m.FinishTask(args.CompletedTask)

	//then give tasks
	for {
		task, result := m.GiveTask()
		switch result {
		case GIVE_TASK_SUCCESS:
			reply.Task = *task
			reply.Done = false
			return nil
		case GIVE_TASK_FAIL:
			m.Cond.Wait()
		case GIVE_TASK_DONE:
			reply.Done = true
			return nil
		}
	}
}

func (m *Master) GiveTask() (*Task, string) {
	//map task first
	now := time.Now().Unix()
	select {
	case mapIndex := <-m.mapChan:
		task := &Task{
			Type: TASK_MAP,
			MapTask: MapTask{
				Filename:     m.files[mapIndex],
				MapIndex:     mapIndex,
				ReduceNumber: m.nReduce,
			},
		}
		m.runningMap[mapIndex] = now
		return task, GIVE_TASK_SUCCESS
	default:
		if len(m.runningMap) > 0 {
			return nil, GIVE_TASK_FAIL
		}
	}

	select {
	case reduceIndex := <-m.reduceChan:
		task := &Task{
			Type: TASK_REDUCE,
			ReduceTask: ReduceTask{
				ReduceIndex: reduceIndex,
				MapNumber:   m.mMap,
			},
		}
		m.runningReduce[reduceIndex] = now
		return task, GIVE_TASK_SUCCESS
	default:
		if len(m.runningReduce) > 0 {
			return nil, GIVE_TASK_FAIL
		}
	}

	return nil, GIVE_TASK_DONE
}

func (m *Master) FinishTask(task Task) {
	switch task.Type {
	case TASK_MAP:
		if _, did := m.runningMap[task.MapTask.MapIndex]; !did {
			return
		}
		delete(m.runningMap, task.MapTask.MapIndex)
		m.completedMap++
		m.Cond.Broadcast()
	case TASK_REDUCE:
		if _, did := m.runningReduce[task.ReduceTask.ReduceIndex]; !did {
			return
		}
		delete(m.runningReduce, task.ReduceTask.ReduceIndex)
		m.completedReduce++
		m.Cond.Broadcast()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	m.Cond.L.Lock()
	defer m.Cond.L.Unlock()
	ret := m.completedMap == m.mMap && m.completedReduce == m.nReduce
	return ret
}

func (m *Master) HandleTimeOut() {
	const TIMEOUT = 10
	for {
		if m.Done() == true {
			return
		}
		m.Cond.L.Lock()
		faile := false
		now := time.Now().Unix()
		for mapIndex, startTime := range m.runningMap {
			if startTime+TIMEOUT < now {
				delete(m.runningMap, mapIndex)
				m.mapChan <- mapIndex
				faile = true
			}
		}
		for reduceIndex, startTime := range m.runningReduce {
			if startTime+TIMEOUT < now {
				delete(m.runningReduce, reduceIndex)
				m.mapChan <- reduceIndex
				faile = true
			}
		}
		if faile {
			m.Cond.Broadcast()
		}
		m.Cond.L.Unlock()
		time.Sleep(time.Second)
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
	m.Cond = sync.NewCond(&m.Mu)
	m.files = files
	m.mMap = len(files)
	m.nReduce = nReduce
	m.mapChan = make(chan int, m.mMap)
	m.reduceChan = make(chan int, m.nReduce)
	m.runningMap = make(map[int]int64, 0)
	m.runningReduce = make(map[int]int64, 0)

	for i := 0; i < m.mMap; i++ {
		m.mapChan <- i
	}

	for i := 0; i < m.nReduce; i++ {
		m.reduceChan <- i
	}

	go m.HandleTimeOut()

	m.server()
	return &m
}
