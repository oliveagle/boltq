package boltq

import (
	"fmt"
	// "github.com/boltdb/bolt"
	// "github.com/kr/pretty"
	"os"
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

	cnt := q2.GetTotalItem()
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

	cnt := q.GetTotalItem()
	if cnt != 1 {
		t.Errorf("TotalItem != 1, %v", cnt)
	}

	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	q.Enqueue([]byte("value"))
	cnt = q.GetTotalItem()
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

	cnt := q.GetTotalItem()
	if cnt != 0 {
		t.Errorf("TotalItem != 0, %v", cnt)
	}

	// t.Error("--")
	teardown()
}

func Test_DequeueAck_NoError(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Enqueue([]byte("value"))

	q.DequeueAck(func(v []byte) error {
		t.Logf("dequeue success: %s", v)
		return nil
	})

	cnt := q.GetTotalItem()
	if cnt != 0 {
		t.Errorf("TotalItem != 0, %v", cnt)
	}
	teardown()
}

func Test_DequeueAck_Error(t *testing.T) {
	teardown()

	q, _ := NewBoltQ(queue_name, 100, ERROR_ON_FULL)
	defer q.Close()

	q.Enqueue([]byte("value"))

	err := q.DequeueAck(func(v []byte) error {
		t.Logf("dequeue fail: %s", v)
		return fmt.Errorf("any error")
	})
	if err == nil {
		t.Error("no error is out")
	}

	cnt := q.GetTotalItem()
	if cnt != 1 {
		t.Errorf("TotalItem != 1, %v", cnt)
	}
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
			t.Log("total_item: ", q.GetTotalItem())
			// t.Error("hahaha fulled")
			break outer_loop
		case <-tick:
			t.Log("total_item: ", q.GetTotalItem())
			if time.Now().Sub(now) > 10*time.Second {
				timed_out <- 0
				t.Error("not fulled")
				break outer_loop
			}
		}
	}
	teardown()
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
			t.Log("total_item: ", q.GetTotalItem())
			t.Error("fulled")
			break outer_loop
		case <-tick:
			t.Log("total_item: ", q.GetTotalItem())
			if time.Now().Sub(now) > 2*time.Second {
				timed_out <- 0
				t.Logf("not fulled, total_count: %d", q.GetTotalItem())
				break outer_loop
			}
		}
	}

	// t.Error("---")
	teardown()
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
