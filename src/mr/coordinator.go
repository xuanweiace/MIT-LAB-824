package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type MRStatus int

const (
	MapPhase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	meta          MRMeta // 只需要支持一个MR任务。（因为是启动的时候传进来的） 如果需要扩展，改一下这里即可。
	worker        map[int]*workerMeta
	gen_worker_id int
	gen_job_id    int
	mu            sync.Mutex
}
type MRMeta struct {
	mapJobs    chan *JobMeta
	reduceJobs chan *JobMeta
	jobs       map[int]JobMeta
	files      []string
	nReduce    int
	status     MRStatus
}

// 超时控制啥的也放在这里
type workerMeta struct {
	id         int
	currentJob JobMeta // 当前worker在执行哪种类型的Job
	// todo lastVisitTime
}

// Your code here -- RPC handlers for the worker to call.

// todo 需要加锁吗？是单线程执行这个的吗？ 应该是需要加锁的
func (c *Coordinator) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	log.Println("[coordinator] RegisterWorker called", c.gen_worker_id)
	reply.Id = c.generate_worker_id()
	c.addWorker(workerMeta{id: reply.Id})
	return nil
}

// 分配任务时要加锁，线程安全的
// 分配任务和完成任务 放到一个函数里了
func (c *Coordinator) AssignJob(req *AssignJobRequest, resp *AssignJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//检查是否有携带已完成任务，有可能发生MR状态转换的
	if req.Job.Status == JobFinish {
		log.Printf("当前任务已完成，开始分发下一个")
		c.finishJob(req.WorkerId, req.Job)
	}
	noJob := getNoJob()
	stopJob := getStopJob()
	if c.meta.status == MapPhase {
		if len(c.meta.mapJobs) > 0 { // 有可分配的map任务
			j := <-c.meta.mapJobs
			log.Printf("[AssignJob] workerId=%d, get MapJobId=%d", req.WorkerId, j.Id)
			resp.Job = j
			j.Status = JobRunning // 这样不会影响resp的status=Init，因为AssignJobResponse里的JobMeta不是指针。
		} else { // 没有可分配的任务且刚刚没有MR状态转换，说明有任务没执行完，让他等待即可。
			resp.Job = &noJob
		}
	} else if c.meta.status == MapPhase {
		if len(c.meta.reduceJobs) > 0 {
			j := <-c.meta.reduceJobs
			log.Printf("[AssignJob] workerId=%d, get ReduceJobId=%d", req.WorkerId, j.Id)
			resp.Job = j
			j.Status = JobRunning
		} else {
			resp.Job = &noJob
		}
	} else { // 如果MR是Done状态了
		resp.Job = &stopJob
	}
	log.Println("[coordinator] AssignJob called")
	return nil
}

// 注意这里不能操作参数job，应该操作c.meta.jobs[job.Id]
// 该方法是线程安全的
func (c *Coordinator) finishJob(workerId int, job JobMeta) {
	c.worker[workerId].currentJob = getEmptyJob()
	//todo 为什么必须要这样写才行？
	x := c.meta.jobs[job.Id]
	x.setFinish()
	//查看是否需要转换状态
	if c.meta.status == MapPhase && c.checkAllMapJobDone() {
		c.meta.status = ReducePhase
	} else if c.meta.status == MapPhase && c.checkAllReduceJobDone() {
		c.meta.status = DonePhase
	}
}
func (c *Coordinator) checkAllMapJobDone() bool {
	for _, j := range c.meta.jobs {
		if j.isMapJob() && !j.isFinish() {
			return false
		}
	}
	return true
}
func (c *Coordinator) checkAllReduceJobDone() bool {
	for _, j := range c.meta.jobs {
		if j.isReduceJob() && !j.isFinish() {
			return false
		}
	}
	return true
}
func (c *Coordinator) generate_worker_id() (ret int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret = c.gen_worker_id
	c.gen_worker_id += 1
	return
}

func (c *Coordinator) generate_job_id() (ret int) {
	ret = c.gen_job_id
	c.gen_job_id += 1
	return
}

// TODO 可以用sync.Map来替代频繁的对map加锁吗？
func (c *Coordinator) addWorker(wk workerMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.worker[wk.id]; ok {
		return fmt.Errorf("[addWorker error!] id=%d 已存在", wk.id)
	}
	c.worker[wk.id] = &wk
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
	log.Printf("Coordinator sockname:%v", sockname)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// 惰性判断isDone，因为只提供这一个接口来判断MapReduce是否结束。
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.meta.status == DonePhase {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		meta: MRMeta{
			mapJobs:    make(chan *JobMeta, len(files)), // 带缓冲channel，来存储任务队列
			reduceJobs: make(chan *JobMeta, nReduce),    // 带缓冲channel，来存储任务队列
			files:      files,
			nReduce:    nReduce,
			status:     MapPhase,
		},
		worker:        map[int]*workerMeta{},
		gen_worker_id: 0,
		gen_job_id:    0,
		mu:            sync.Mutex{},
	}
	c.initJobs()
	log.Println("00223")

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) initJobs() {
	for _, v := range c.meta.files {

		c.meta.mapJobs <- &JobMeta{
			Id:       c.generate_job_id(),
			Type:     MapJob,
			Filename: v,
			Status:   JobInit,
			NReduce:  c.meta.nReduce,
		}
	}

	//todo  init reduce jobs
	for i := 0; i < c.meta.nReduce; i++ {
		c.meta.reduceJobs <- &JobMeta{
			Id:        c.generate_job_id(),
			Type:      MapJob,
			Status:    JobInit,
			NReduce:   c.meta.nReduce,
			JobReduce: i,
		}
	}
}
