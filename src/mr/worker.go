// @Update  lijin
// @Date  2022-10-8
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//从mrsequential.go中参考
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		//请求分配任务
		args := ExampleArgs{}
		reply := Task{}
		call("Coordinator.Assign", &args, &reply)
		switch reply.TaskState {
		case Map:
			mapper(&reply, mapf)
		case Reduce:
			reduce(&reply, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}

func mapper(reply *Task, mapf func(string, string) []KeyValue) {
	context, err := ioutil.ReadFile(reply.File)
	if err != nil {
		log.Fatal(err)
	}

	keyvalues := mapf(reply.File, string(context)) //返回所有的keyvalue切片
	temp := make([][]KeyValue, reply.NReduce)      //将所有的keyvalue分组
	for _, kv := range keyvalues {
		index := ihash(kv.Key) % reply.NReduce //利用ihash生成中间文件的位置，实验要求
		temp[index] = append(temp[index], kv)
	} //这样我们就把map的结果放在了n个文件中，方便n个reduce进行处理

	op := make([]string, 0)
	for i := 0; i < reply.NReduce; i++ {
		op = append(op, writeToLocalFile(reply.TaskId, i, &temp[i]))
	}
	reply.Temp = op //将文件位置保存在Task中

	r := ExampleReply{}
	call("Coordinator.Completed", reply, &r) //rpc通知coordinator任务完成
}

func reduce(task *Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{} //reduce处理的时keyvalue，我们需要将文件序列化为keyvalue
	for _, file := range task.Temp {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal("Failed to open file", err)
		}
		con := json.NewDecoder(f)
		for {
			var t KeyValue
			if err := con.Decode(&t); err != nil {
				break
			}
			intermediate = append(intermediate, t)
		}
		f.Close()
	}
	//下面可以参考mrsequential.go
	sort.Sort(ByKey(intermediate))
	src, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(src, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	newname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), newname)
	task.Output = newname

	r := ExampleReply{}

	call("Coordinator.Completed", task, &r) //rpc通知coordinator任务完成
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
