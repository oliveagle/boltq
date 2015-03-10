package boltq

import (
	"fmt"
	// "github.com/boltdb/bolt"
	// "github.com/kr/pretty"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	queue_name = "test_q.queue"
)

func teardown() {
	os.Remove(queue_name)
}

func Test_boltq_another_instance_GetTotalItem_above_zero(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	q.Close()

	// another instance
	q2, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q2.Close()

	cnt := q2.Size()
	if cnt != 3 {
		t.Errorf("TotalItem != 3, %v", cnt)
	}
	// t.Error("--")
	teardown()
}

func Test_boltq_Enqueue_GetTotalItem_1(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Enqueue([]byte("value"))

	cnt := q.Size()
	if cnt != 1 {
		t.Errorf("TotalItem != 1, %v", cnt)
	}

	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	cnt = q.Size()
	if cnt != 4 {
		t.Errorf("TotalItem != 4, %v", cnt)
	}

	// t.Error("--")
	teardown()
}

func Test_boltq_Dequeue_GetTotalItem_0(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Enqueue([]byte("value"))
	v, _ := q.Dequeue()
	if string(v) != "value" {
		t.Errorf("different value: %s != value ", v)
	}

	cnt := q.Size()
	if cnt != 0 {
		t.Errorf("TotalItem != 0, %v", cnt)
	}

	// t.Error("--")
	teardown()
}

func Test_Dequeue_Empty(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	// q.Enqueue([]byte("value"))

	value, err := q.Dequeue()
	t.Log(value, err)
	if err == nil {
		t.Error("Dequeue empty no error")
	}

	cnt := q.Size()
	if cnt != 0 {
		t.Errorf("TotalItem != 0, %v", cnt)
	}
	// t.Error("hhh")
	teardown()
}

func Test_Full_On_Error(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()
	timed_out := make(chan int)
	fulled := make(chan int)

	go func() {
		tick := time.Tick(10 * time.Millisecond)
	loop:
		for {
			select {
			case <-tick:
				for i := 0; i < 1000; i++ {
					err := q.Enqueue([]byte("value"))
					if err != nil && strings.HasPrefix(err.Error(), "Queue is full") {
						t.Log("error: ", err)
						fulled <- 0
						break loop
					}
				}
			case <-timed_out:
				break loop
			}
		}

	}()

	tick := time.Tick(1000 * time.Millisecond)
	now := time.Now()
outer_loop:
	for {
		select {
		case <-fulled:
			// fmt.Println("Fulled")
			t.Log("total_item: ", q.Size())
			// t.Error("hahaha fulled")
			break outer_loop
		case <-tick:
			t.Log("total_item: ", q.Size())
			if time.Now().Sub(now) > 10*time.Second {
				timed_out <- 0
				t.Error("not fulled")
				break outer_loop
			}
		}
	}
	teardown()
}

func Test_Pop_And_Dequeue(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Enqueue([]byte("1"))
	q.Enqueue([]byte("2"))
	q.Enqueue([]byte("3"))

	v, _ := q.Dequeue()
	t.Logf("%s", v)
	if fmt.Sprintf("%s", v) != "1" {
		t.Error("Dequeue Error")
	}

	v, _ = q.Pop()
	t.Logf("%s", v)
	if fmt.Sprintf("%s", v) != "3" {
		t.Error("Pop Error")
	}

	// t.Error("---")
}

func Test_Push_And_Pop(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Push([]byte("1"))
	q.Push([]byte("2"))
	q.Push([]byte("3"))

	v, _ := q.Pop()
	t.Logf("%s", v)
	if fmt.Sprintf("%s", v) != "3" {
		t.Error("Pop Error")
	}

	v, _ = q.Pop()
	t.Logf("%s", v)
	if fmt.Sprintf("%s", v) != "2" {
		t.Error("Pop Error")
	}

	v, _ = q.Pop()
	t.Logf("%s", v)
	if fmt.Sprintf("%s", v) != "1" {
		t.Error("Pop Error")
	}
	// t.Error("---")
}

func Test_Full_Pop(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, POP_ON_FULL)
	defer q.Close()
	timed_out := make(chan int)
	fulled := make(chan int)

	go func() {
		tick := time.Tick(10 * time.Millisecond)
	loop:
		for {
			select {
			case <-tick:
				for i := 0; i < 1000; i++ {
					// q.Enqueue("value")
					err := q.Enqueue([]byte("value"))
					if err != nil && strings.HasPrefix(err.Error(), "Queue is full") {
						t.Log("error: ", err)
						fulled <- 0
						break loop
					}
				}
			case <-timed_out:
				break loop
			}
		}

	}()

	tick := time.Tick(1000 * time.Millisecond)
	now := time.Now()
outer_loop:
	for {
		select {
		case <-fulled:
			// fmt.Println("Fulled")
			t.Log("total_item: ", q.Size())
			t.Error("fulled")
			break outer_loop
		case <-tick:
			t.Log("total_item: ", q.Size())
			if time.Now().Sub(now) > 2*time.Second {
				timed_out <- 0
				t.Logf("not fulled, total_count: %d", q.Size())
				break outer_loop
			}
		}
	}

	// t.Error("---")
	teardown()
}

func Test_popmany_false(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Push([]byte("1"))
	q.Push([]byte("2"))
	q.Push([]byte("3"))

	q.popmany(false, func(v []byte) bool {
		return false
	})
	if q.Size() != 3 {
		t.Error("should not pop any thing")
	}
}

func Test_popmany_true_all(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Push([]byte("1"))
	q.Push([]byte("2"))
	q.Push([]byte("3"))

	q.popmany(false, func(v []byte) bool {
		return true
	})
	if q.Size() != 0 {
		t.Error("should pop all")
	}
}

func Test_popmany_true_partial_top(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Push([]byte("1"))
	q.Push([]byte("2"))
	q.Push([]byte("3"))

	err := q.PopMany(func(v []byte) bool {
		fmt.Println(fmt.Sprintf("%s", v))
		i, _ := strconv.Atoi(fmt.Sprintf("%s", v))
		fmt.Println(i)
		if i > 1 {
			return true
		}
		// when popmany saw the first `false`, it will break and return
		return false
	})
	if err != nil {
		t.Errorf("err: %s", err)
	}
	if q.Size() != 1 {
		t.Log(q.Size())
		t.Error("should pop 2 items only")
	}
	v, _ := q.Pop()
	if fmt.Sprintf("%s", v) != "1" {
		t.Error("pop wrong direction")
	}
}

func Test_popmany_empty(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	err := q.PopMany(func(v []byte) bool {
		t.Error("should not come to here.")
		return true
	})
	if err == nil || err.Error() != "Queue is empty" {
		t.Errorf("should raise Queue is Empty error")
	}
}

func Test_popmany_true_partial_bottom(t *testing.T) {
	teardown()
	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Push([]byte("1"))
	q.Push([]byte("2"))
	q.Push([]byte("3"))

	err := q.PopManyBottom(func(v []byte) bool {
		fmt.Println(fmt.Sprintf("%s", v))
		i, _ := strconv.Atoi(fmt.Sprintf("%s", v))
		fmt.Println(i)
		if i < 3 {
			return true
		}
		// when popmany saw the first `false`, it will break and return
		return false
	})
	if err != nil {
		t.Errorf("err: %s", err)
	}
	if q.Size() != 1 {
		t.Log(q.Size())
		t.Error("should pop 2 items only")
	}
	v, _ := q.Pop()
	if fmt.Sprintf("%s", v) != "3" {
		t.Error("pop wrong direction")
	}
}

func BenchmarkEnqueue(b *testing.B) {
	teardown()

	q, _ := NewBoltQ(queue_name, 1000000, ERROR_ON_FULL)
	defer q.Close()

	for i := 0; i < b.N; i++ {
		q.Enqueue([]byte("value"))
	}
	teardown()
}

func BenchmarkEnqueuePop(b *testing.B) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, POP_ON_FULL)
	defer q.Close()

	for i := 0; i < b.N; i++ {
		err := q.Enqueue([]byte("value"))
		if err != nil {
			b.Error("should not raise any error here")
		}
	}
	teardown()
}
