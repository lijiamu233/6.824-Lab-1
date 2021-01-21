package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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
	args := &ApplyForTaskArgs{}
	for {
		reply, ok := ApplyForTask(args)
		if !ok || reply.Done {
			break
		}
		task := &reply.Task

		if task.Type == TASK_MAP {
			mapTask := task.MapTask
			DoMap(mapTask.Filename, mapTask.MapIndex, mapTask.ReduceNumber, mapf)
		} else if task.Type == TASK_REDUCE {
			reduceTask := task.ReduceTask
			DoReduce(reduceTask.ReduceIndex, reduceTask.MapNumber, reducef)
		}
		args.CompletedTask = *task
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func ApplyForTask(args *ApplyForTaskArgs) (*ApplyForTaskReply, bool) {
	reply := &ApplyForTaskReply{}
	flag := call("Master.ApplyForTask", args, reply)
	return reply, flag
}

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

//Following is DoMap and DoReduce
//from mysequential.go

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func DoMap(fileName string, mapIndex int, reduceNumber int, mapf func(string, string) []KeyValue) {
	Map := make([]ByKey, reduceNumber)

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("open %v failed", fileName)
	}

	container, err := ioutil.ReadAll(file)
	file.Close()

	kva := mapf(fileName, string(container))

	for _, item := range kva {
		index := ihash(item.Key) % reduceNumber
		Map[index] = append(Map[index], item)
	}

	for reduceIndex, bucket := range Map {
		file, err := ioutil.TempFile("/home/lijiamu/下载/Lab_newbie_task/6.824Lab1/6.824/tmp/", "map-tmp")
		if err != nil {
			log.Fatalf("tmp error: %v", err)
		}
		encode := json.NewEncoder(file)
		for _, kv := range bucket {
			err := encode.Encode(&kv)
			if err != nil {
				log.Fatalf("json error: %v", err)
			}
		}
		filename := fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
		err = os.Rename(file.Name(), filename)
		if err != nil {
			log.Fatalf("rename fail: %v", err)
		}
	}
}

func DoReduce(reduceIndex int, mapNumber int, reducef func(string, []string) string) {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < mapNumber; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reduceIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("open intermediate error: %v", err)
		}
		decode := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decode.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	outfileName := fmt.Sprintf("mr-out-%d", reduceIndex)
	tmp, _ := ioutil.TempFile("/home/lijiamu/下载/Lab_newbie_task/6.824Lab1/6.824/tmp/", "reduce-tmp")

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmp.Close()
	os.Rename(tmp.Name(), outfileName)
}
