// @Update  lijin
// @Date  2022-10-8
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

type Status int
type Rstatus int

const (
	Map Status = iota
	Reduce
	Exit
	Wait
)
const (
	Idle Rstatus = iota
	Running
	Complete
)

type Coordinator struct {
	TaskQueue chan *Task
	TaskInfo  map[int]*TaskInfo
	NReduce   int        //reduce的数量
	Files     []string   //输入文件
	Temp      [][]string //存放将要reduce的信息
	Exit      bool       //mrcoordinator会周期检查是否退出
}

type TaskInfo struct {
	RStatus   Rstatus //是否被处理
	StartTime time.Time
	Task      *Task
}

/*
map和reduce两个阶段需要的任务数据结构其实不太一样
map处理一个文件产生n个中间，最后我们吧所有的中间数据，分为n组
每组的数量不定，这个是ihash产生的，假设每个有k
reduce则需要处理k个文件产生一个结果，n个reduce，一共有n个最终文件

但是为了方便我们使用一个结构体
*/
type Task struct {
	TaskId    int
	File      string //文件位置，mapper阶段用
	TaskState Status //需要做哪一步处理
	NReduce   int
	Temp      []string //用于保存每个work产生的n个文件、进行reduce时处理的文件
	Output    string   //输出文件名，reduce阶段用
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//启动RPC服务
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.Exit
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{ //注意不要忘了切片的初始化，否则后面会越界
		TaskQueue: make(chan *Task, max(nReduce, len(files))),
		TaskInfo:  make(map[int]*TaskInfo),
		NReduce:   nReduce,
		Files:     files,
		Temp:      make([][]string, nReduce),
		Exit:      false,
	}
	//初始化任务
	for id, file := range files {
		task := Task{
			TaskId:    id,
			File:      file,
			TaskState: Map,
			NReduce:   nReduce,
		}
		c.TaskInfo[id] = &TaskInfo{
			RStatus: Idle,
			Task:    &task,
		}
		c.TaskQueue <- &task
	}
	c.server()
	go c.heartBeat() //超时检查
	return &c
}

var m sync.Mutex

//分配任务
func (c *Coordinator) Assign(args *ExampleArgs, reply *Task) error {
	m.Lock()
	defer m.Unlock()
	if len(c.TaskQueue) > 0 { //还有任务
		*reply = *<-c.TaskQueue                    //队列汇总取一个任务
		c.TaskInfo[reply.TaskId].RStatus = Running //修改状态
		c.TaskInfo[reply.TaskId].StartTime = time.Now()
	} else if c.Exit { //coordinator退出
		*reply = Task{TaskState: Exit}
	} else { //没有任务
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (c *Coordinator) Completed(task *Task, reply *ExampleReply) error {
	m.Lock()
	defer m.Unlock()
	if c.TaskInfo[task.TaskId].RStatus == Complete { //这个文件已经被别的worker处理完了,针对超时
		return nil
	}
	c.TaskInfo[task.TaskId].RStatus = Complete //设置完成状态
	/*
		每个worker都会产生n个中间文件，我们需要将这些信息汇总给coordinater，方便之后的任务分配
		每个任务完成时机，也是我们检查是不是所有任务都完成的时机，如果都完成了，就退出
		这个过程要完成任务很多，代码还要上锁，为了提高效率，我们开新的协程
	*/
	go c.GenerateResult(task)
	return nil
}

func (c *Coordinator) GenerateResult(task *Task) { //合并中间结果，检查所有任务是否完成
	m.Lock()
	defer m.Unlock()
	switch task.TaskState {
	case Map: //map结束，进入reduce阶段
		for id, file := range task.Temp { //将中间结果交给coordinator进行处理
			c.Temp[id] = append(c.Temp[id], file)
		}
		if c.IsAllDown() { //所有任务进入reduce
			c.TaskInfo = make(map[int]*TaskInfo)
			for id, file := range c.Temp { //初始化任务信息
				task := Task{
					TaskId:    id,
					TaskState: Reduce,
					NReduce:   c.NReduce,
					Temp:      file,
				}
				c.TaskQueue <- &task
				c.TaskInfo[id] = &TaskInfo{
					RStatus: Idle,
					//StartTime: time.Now(),    reduce阶段没有做时间要求
					Task: &task,
				}

			}
		}
	case Reduce:
		if c.IsAllDown() {
			c.Exit = true //退出coordinator
		}
	}
}

func (c *Coordinator) IsAllDown() bool {
	for _, t := range c.TaskInfo {
		if t.RStatus != Complete { //有任务没有完成
			return false
		}
	}
	return true
}

func (c Coordinator) heartBeat() {
	for {
		m.Lock()
		if c.Exit { //coordinator退出
			m.Unlock()
			return
		}
		for _, taks := range c.TaskInfo {
			if taks.RStatus == Running && time.Now().Sub(taks.StartTime) > time.Second*10 {
				c.TaskQueue <- taks.Task //将这个任务放回任务队列中
				taks.RStatus = Idle      //设置空闲状态
			}
		}
		m.Unlock()
		time.Sleep(time.Second * 5)
	}
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
