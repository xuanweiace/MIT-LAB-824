package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	worker_id := CallRegisterWorker()
	fmt.Println("远程调用获得的id=", worker_id)
	job := getEmptyJob()
	keep_working := true
	for keep_working {
		job = CallAssignJob(worker_id, job)
		log.Printf("[Worker] worker=%d, get job=%+v", worker_id, job)
		// 对job进行操作
		switch job.Type {
		case MapJob:
			executeMapJob(job, mapf)
		case ReduceJob:
			executeReduceJob(job, reducef)
		case Nojob:
			time.Sleep(time.Second)
		case StopJob:
			keep_working = false
		}
		job.setFinish()
		log.Printf("[Worker] worker=%d, finish job=%+v", worker_id, job)
		time.Sleep(time.Second)
	}
}
func executeMapJob(job JobMeta, mapf func(string, string) []KeyValue) {
	var intermediate []KeyValue

	file, err := os.Open(job.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", job.Filename)
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", job.Filename)
	}
	intermediate = mapf(job.Filename, string(content))
	t_map := make(map[string][]KeyValue, job.NReduce)
	for _, entry := range intermediate {
		s := generate_output_file(job.Id, ihash(entry.Key)%job.NReduce)
		if _, ok := t_map[s]; !ok {
			t_map[s] = make([]KeyValue, 0)
		}
		t_map[s] = append(t_map[s], entry)
	}
	for oname, entry_list := range t_map {
		ofile, _ := os.Create(oname)
		for _, entry := range entry_list {
			fmt.Fprintf(ofile, "%v %v\n", entry.Key, entry.Value)
		}
		ofile.Close()
	}
}
func generate_output_file(jobid, reduceid int) string {
	return fmt.Sprintf("./mr-tmp/mr-tmp-%d-%d", jobid, reduceid)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func executeReduceJob(job JobMeta, reducef func(string, []string) string) {
	intermediate := readFromLocalFileByJobReduce(job.JobReduce)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	ofile, err := ioutil.TempFile(dir, "mr-tmp-*") // 先写入临时文件，因为如果崩溃了，可以防止被别的进程观察到
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", job.JobReduce)) // 重命名成真正文件
}
func readFromLocalFileByJobReduce(jobReduce int) []KeyValue {
	res := []KeyValue{}
	path, _ := os.Getwd()
	path += "/mr-tmp/"
	rd, _ := ioutil.ReadDir(path)
	println("rd:", rd)
	for _, fi := range rd {
		// println("fi.Name():", fi.Name())
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(jobReduce)) {
			res = append(res, readFromFile(path+fi.Name())...)
		}
	}
	return res
}
func readFromFile(filename string) []KeyValue {
	res := []KeyValue{}
	file, _ := os.Open(filename)
	defer file.Close()
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text()
		// println("line:", line)
		li := strings.Split(line, " ")
		res = append(res, KeyValue{
			Key:   li[0],
			Value: li[1],
		})
	}
	return res
}
func CallAssignJob(workerId int, done_job JobMeta) JobMeta {
	req := AssignJobRequest{
		WorkerId: workerId,
		Job:      done_job,
	}
	resp := AssignJobResponse{}
	log.Printf("[CallAssignJob] workerId=%d, 即将远程调用AssignJob", req.WorkerId)
	ok := call("Coordinator.AssignJob", &req, &resp)
	if ok {
		return *resp.Job
	} else {
		log.Printf("[CallAssignJob] workerId=%d, 调用AssignJob失败", req.WorkerId)
		return getNoJob() // 调用失败，也返回NoJob，睡眠后重新获取
	}
}
func CallRegisterWorker() int {
	req := RegisterWorkerRequest{}
	resp := RegisterWorkerResponse{}
	fmt.Println("即将远程调用RegisterWorker")
	ok := call("Coordinator.RegisterWorker", &req, &resp)
	fmt.Println("resp:", resp)
	if ok {
		return resp.Id
	} else {
		return -1
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
