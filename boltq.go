// boltq is a very simple boltdb based embedded queue
//
// ```
// package main
//
// import "github.com/oliveagle/boltq"
//
// func main() {
// 		q, err := boltq.NewBoltQ("test_q.queue", 1, boltq.ERROR_ON_FULL)
//		defer q.Close()
//
//		q.Enqueue([]byte("value"))
//		value, _ := q.Dequeue([]byte("value"))
// }
// ```
//
// more infomation: http://github.com/oliveagle/boltq

package boltq

import (
	"fmt"
	"github.com/boltdb/bolt"
	// "log"
	"strconv"
	"sync"
	"time"
)

const (
	// raise an error if the queue reached `max_queue_size`
	// popout oldest item if queue size above `max_queue_size`
	ERROR_ON_FULL = iota
	POP_ON_FULL
)

const (
	BUCKET_STATS  = "~~stats~~"
	BUCKET_QUEUE  = "queue"
	KEY_TOTALITEM = "TotalItemCount"
)

type BoltQ struct {
	max_queue_size int64
	filename       string
	db             *bolt.DB
	onfull         int
	total_item     int64
	last_nanosec   int64
	last_collision int
	mutex          sync.RWMutex
	keylock        sync.RWMutex
}

func NewBoltQ(filename string, max_queue_size int64, onfull int) (*BoltQ, error) {
	db, err := bolt.Open(filename, 0660, nil)
	if err != nil {
		return nil, err
	}
	if onfull > POP_ON_FULL || onfull < ERROR_ON_FULL {
		return nil, fmt.Errorf("onful can only be: ERROR_ON_FULL, POP_ON_FULL")
	}

	total_item, err := getTotalItemFromDB(db)
	if err != nil {
		return nil, err
	}

	return &BoltQ{
		max_queue_size: max_queue_size,
		filename:       filename,
		db:             db,
		onfull:         onfull,
		total_item:     total_item,
	}, nil
}

func getTotalItemFromDB(db *bolt.DB) (int64, error) {
	total_item := int64(0)
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BUCKET_STATS))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(KEY_TOTALITEM))
		if v == nil {
			return nil
		}
		tmp, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			fmt.Printf("ERROR: parse count %v\n", err)
			return err
		}

		total_item = tmp
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total_item, nil
}

func (b *BoltQ) Close() {
	b.db.Close()
}

func (b *BoltQ) GetTotalItem() int64 {
	return b.total_item
}

func (b *BoltQ) IsFull() bool {
	return int64(b.total_item) >= b.max_queue_size
}

func (b *BoltQ) SetMaxQueueSize(size int64) {
	if size > 0 {
		b.max_queue_size = size
	}
}

func (b *BoltQ) Enqueue(value []byte) (err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if !b.IsFull() {
		// b.newKey()
		// 1182 ns/op

		key := b.newKey()
		err = b.db.Update(func(tx *bolt.Tx) error {
			// 260214 ns/op

			// tx.CreateBucketIfNotExists([]byte(BUCKET_QUEUE))
			// 270968 ns/op

			bkt, err := tx.CreateBucketIfNotExists([]byte(BUCKET_QUEUE))
			if err != nil {
				return err
			}
			err = bkt.Put(key, value)
			if err != nil {
				return err
			}
			// 392482 ns/op
			return b.increaseTotalItem(tx)
		})
	} else {
		switch b.onfull {
		case ERROR_ON_FULL:
			err = fmt.Errorf("Queue is full on size: %d >= %d", b.GetTotalItem(), b.max_queue_size)
			// fmt.Println(err)
		case POP_ON_FULL:
			// fmt.Println("pop_on_full: not implemented")
			// err = fmt.Errorf("pop_on_full: not implemented")
			key := b.newKey()
			err = b.db.Update(func(tx *bolt.Tx) error {

				bkt, err := tx.CreateBucketIfNotExists([]byte(BUCKET_QUEUE))
				if err != nil {
					return err
				}
				// delete first one
				c := bkt.Cursor()
				k, _ := c.First()
				err = bkt.Delete(k)
				if err != nil {
					return err
				}

				err = bkt.Put(key, value)
				if err != nil {
					return err
				}
				return err
			})
		default:
			err = fmt.Errorf("onful can only be: ERROR_ON_FULL, POP_ON_FULL")
		}
	}
	return err
}

func (b *BoltQ) Dequeue() (value []byte, err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	err = b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(BUCKET_QUEUE))
		if err != nil {
			return err
		}
		c := bkt.Cursor()
		k, v := c.First()
		err = bkt.Delete(k)
		if err != nil {
			return err
		}
		err = b.decreaseTotalItem(tx)
		if err != nil {
			return err
		}
		value = v
		return nil
	})
	return value, err
}

func (b *BoltQ) DequeueAck(ack func(value []byte) error) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(BUCKET_QUEUE))
		if err != nil {
			return err
		}
		c := bkt.Cursor()
		k, v := c.First()

		// ack here
		err = ack(v)
		if err != nil {
			return err
		}

		// acked, delete it
		err = bkt.Delete(k)
		if err != nil {
			return err
		}
		return b.decreaseTotalItem(tx)
	})
}

func (b *BoltQ) newKey() []byte {
	b.keylock.Lock()
	defer b.keylock.Unlock()

	now := time.Now().UnixNano()
	if now == b.last_nanosec {
		b.last_collision += 1
	} else {
		b.last_collision = 0
		b.last_nanosec = now
	}

	// generate a unique new key
	return []byte(fmt.Sprintf("%d%04d", b.last_nanosec, b.last_collision))
}

func (b *BoltQ) increaseTotalItem(tx *bolt.Tx) (err error) {
	stats_bucket, err := tx.CreateBucketIfNotExists([]byte(BUCKET_STATS))
	tmp_count := stats_bucket.Get([]byte(KEY_TOTALITEM))
	if tmp_count != nil {
		tmp, err := strconv.ParseInt(string(tmp_count), 10, 64)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return err
		}
		b.total_item = tmp
	}
	// remove above codes won't boost performance ...

	b.total_item += 1
	err = stats_bucket.Put([]byte(KEY_TOTALITEM), []byte(fmt.Sprintf("%d", b.total_item)))
	return
}

func (b *BoltQ) decreaseTotalItem(tx *bolt.Tx) (err error) {
	stats_bucket, err := tx.CreateBucketIfNotExists([]byte(BUCKET_STATS))
	tmp_count := stats_bucket.Get([]byte(KEY_TOTALITEM))
	if tmp_count != nil {
		tmp, err := strconv.ParseInt(string(tmp_count), 10, 64)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return err
		}
		b.total_item = tmp
	}
	// remove above codes won't boost performance ...

	b.total_item -= 1
	err = stats_bucket.Put([]byte(KEY_TOTALITEM), []byte(fmt.Sprintf("%d", b.total_item)))
	return
}
