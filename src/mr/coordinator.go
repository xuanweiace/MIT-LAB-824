package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
	jobs       map[int]*JobMeta
	files      []string
	nReduce    int
	status     MRStatus
}

// 超时控制啥的也放在这里
type workerMeta struct {
	id            int
	currentJob    JobMeta // 当前worker在执行哪种类型的Job
	lastVisitTime time.Time
	online        bool
}

func (w workerMeta) workerDead() bool {
	return time.Now().Sub(w.lastVisitTime).Seconds() > 10
}

// Your code here -- RPC handlers for the worker to call.

// todo 需要加锁吗？是单线程执行这个的吗？ 应该是需要加锁的
func (c *Coordinator) RegisterWorker(args *RegisterWorkerRequest, reply *RegisterWorkerResponse) error {
	reply.Id = c.generate_worker_id()
	// log.Println("[coordinator] RegisterWorker called, assign workerid=", reply.Id)
	c.addWorker(workerMeta{id: reply.Id})
	return nil
}

// 分配任务时要加锁，线程安全的
// 分配任务和完成任务 放到一个函数里了
func (c *Coordinator) AssignJob(req *AssignJobRequest, resp *AssignJobResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if canWelcome := c.workerCome(req.WorkerId); !canWelcome {
		resp.Job = getStopJob()
		return fmt.Errorf("You are offline.")
	}
	//检查是否有携带已完成任务，有可能发生MR状态转换的
	// 只处理map和reduce两种job即可
	if req.Job.Status == JobFinish && (req.Job.isMapJob() || req.Job.isReduceJob()) {
		// log.Printf("当前任务%d已完成，开始分发下一个", req.Job.Id)
		c.finishJob(req.WorkerId, req.Job)
	}

	if c.meta.status == MapPhase {
		if len(c.meta.mapJobs) > 0 { // 有可分配的map任务
			j := <-c.meta.mapJobs
			// log.Printf("[AssignJob] workerId=%d, assigned MapJobId=%d", req.WorkerId, j.Id)
			resp.Job = *j
			j.Status = JobRunning // 这样不会影响resp的status=Init，因为AssignJobResponse里的JobMeta不是指针。
		} else { // 没有可分配的任务且刚刚没有MR状态转换，说明有任务没执行完，让他等待即可。
			resp.Job = getNoJob()
		}
	} else if c.meta.status == ReducePhase {
		if len(c.meta.reduceJobs) > 0 {
			j := <-c.meta.reduceJobs
			// log.Printf("[AssignJob] workerId=%d, assigned ReduceJobId=%d", req.WorkerId, j.Id)
			resp.Job = *j
			j.Status = JobRunning
		} else {
			resp.Job = getNoJob()
		}
	} else { // 如果MR是Done状态了
		resp.Job = getStopJob()
	}
	c.worker[req.WorkerId].currentJob = resp.Job
	// 如果是noJob，查看是不是有worker断掉了，但是任务没完成
	if resp.Job.isNoJob() {
		c.replayJobChecker()
	}
	return nil
}
func (c *Coordinator) workerCome(workerId int) bool {
	//状态越少，实现越简单，所以这里断连，直接认为worker下线，如果重连，则认为是新worker
	if workermeta, ok := c.worker[workerId]; !ok {
		return false
	} else if workermeta == nil {
		c.worker[workerId] = &workerMeta{
			id:            workerId,
			currentJob:    getEmptyJob(),
			lastVisitTime: time.Now(),
			online:        true,
		}
	}
	c.worker[workerId].lastVisitTime = time.Now()
	return true
}
func (c *Coordinator) replayJobChecker() {
	for _, v := range c.worker {
		if v.workerDead() && (v.currentJob.isMapJob() || v.currentJob.isReduceJob()) {
			delete(c.worker, v.id)
			c.replayJob(v.currentJob)
		}
	}

}
func (c *Coordinator) replayJob(job JobMeta) {
	if job.isMapJob() {
		c.meta.mapJobs <- &job
	} else if job.isReduceJob() {
		c.meta.reduceJobs <- &job
	}
}

// 注意这里不能操作参数job，应该操作c.meta.jobs[job.Id]
// 该方法是线程安全的
func (c *Coordinator) finishJob(workerId int, job JobMeta) {
	c.worker[workerId].currentJob = getEmptyJob()
	c.meta.jobs[job.Id].setFinish()
	//查看是否需要转换状态
	if c.meta.status == MapPhase && c.checkAllMapJobDone() {
		// log.Printf("状态转换到Reduce Phase")
		c.meta.status = ReducePhase
	} else if c.meta.status == ReduceJob && c.checkAllReduceJobDone() {
		// log.Printf("状态转换到Done Phase")
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
	// log.Printf("Coordinator sockname:%v", sockname)
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
			mapJobs:    make(chan *JobMeta, len(files)),
			reduceJobs: make(chan *JobMeta, nReduce),
			jobs:       make(map[int]*JobMeta, len(files)+nReduce),
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

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) initJobs() {
	for _, v := range c.meta.files {
		j := &JobMeta{
			Id:       c.generate_job_id(),
			Type:     MapJob,
			Filename: v,
			Status:   JobInit,
			NReduce:  c.meta.nReduce,
		}
		c.meta.jobs[j.Id] = j
		c.meta.mapJobs <- j
	}

	for i := 0; i < c.meta.nReduce; i++ {
		j := &JobMeta{
			Id:        c.generate_job_id(),
			Type:      ReduceJob,
			Status:    JobInit,
			NReduce:   c.meta.nReduce,
			JobReduce: i,
		}
		c.meta.jobs[j.Id] = j
		c.meta.reduceJobs <- j
	}
	// log.Printf("[initJobs] init %d mapjob and %d reducejob", len(c.meta.files), c.meta.nReduce)
}
