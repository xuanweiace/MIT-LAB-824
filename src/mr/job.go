package mr

// worker 和 coordinator 共享的
type JobType int

const (
	MapJob = iota
	ReduceJob
	Nojob // 不能长链接阻塞等待任务，所以如果没有任务可做，就会返回给worker一个NoJob
	StopJob
)

type JobStutus int

const (
	JobInit = iota
	JobRunning
	JobFinish
)

type JobMeta struct {
	Id        int
	Type      JobType
	Filename  string
	Status    JobStutus
	NReduce   int
	JobReduce int
}

var emptyJob JobMeta

func getEmptyJob() JobMeta {
	emptyJob.Id = -1
	return emptyJob
}
func getNoJob() JobMeta {
	return JobMeta{
		Id:   0,
		Type: Nojob,
	}
}

func getStopJob() JobMeta {
	return JobMeta{
		Id:   0,
		Type: StopJob,
	}
}

func isEmptyJob(j JobMeta) bool {
	return j.Id == -1
}

func (j JobMeta) isMapJob() bool {
	return j.Type == MapJob
}

func (j JobMeta) isReduceJob() bool {
	return j.Type == ReduceJob
}
func (j JobMeta) isNoJob() bool {
	return j.Type == Nojob
}
func (j JobMeta) isStopJob() bool {
	return j.Type == StopJob
}

// 必须是指针
func (j *JobMeta) setFinish() {
	j.Status = JobFinish
}

func (j *JobMeta) isFinish() bool {
	return j.Status == JobFinish
}
