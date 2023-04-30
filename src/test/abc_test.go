package test

import (
	"fmt"
	"sync"
	"testing"
)

func TestABC(t *testing.T) {
	mu := sync.Mutex{}
	mu.Lock()
	fmt.Println(1)
	mu.Lock() /// 会阻塞
	fmt.Println(2)
	t.Fail()

}

type MRStatus int

var x MRStatus

func TestType(t *testing.T) {
	x = 5
	fmt.Println(x == 5) // 这样是可以的
}

type A int

func (a *A) test() {
	fmt.Println("test", int(*a))
	*a = 5

}
func TestPoint(t *testing.T) {
	m := map[int]A{}
	m[0] = A(1)
	// tmp := &m[0]
	// m[0].test()
	x := m[0]
	x.test()
	fmt.Println("x=", x, ", m[0]=", m[0])
}

type AssignJobResponse struct {
	Job JobMeta
}
type JobMeta struct {
	Id int
}

func TestPoin2(t *testing.T) {

	resp := &AssignJobResponse{}
	j := &JobMeta{Id: 2}
	resp.Job = *j
	j.Id = 3
	fmt.Println(resp.Job.Id)
	t.Fail()
}
