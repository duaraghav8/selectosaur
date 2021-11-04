package main

import (
	"fmt"
	"time"
)

const maxWorkers = 1000

type Result struct {
	Job         *QueryParameter
	Err         error
	ElapsedTime time.Duration
}

// WorkerPool manages a pool of workers to perform multiple timescale query
//execution jobs concurrently.
// It guarantees that for every job submitted, there will be exactly 1 Result
// returned via its results channel.
type WorkerPool struct {
	db        *Datastore
	count     int
	resultsCh chan *Result
}

func (wp *WorkerPool) ResultsCh() <-chan *Result {
	return wp.resultsCh
}

// Submit accepts a query parameter as a job to execute.
// This method never blocks. If a suitable worker is immediately available,
// the job is assigned to it, otherwise it is inserted into an internal job queue.
func (wp *WorkerPool) Submit(qp *QueryParameter) {
	// Determine target worker ID using modulus operation.
	// This guarantees that queries of the same host go to the same worker each time.
	wid := qp.HostID() % wp.count
}

func newWorkerPool(count int, db *Datastore) (*WorkerPool, error) {
	if count < 1 || count > maxWorkers {
		return nil, fmt.Errorf("worker count should be between 1 and %d", maxWorkers)
	}
	p := &WorkerPool{
		db:        db,
		count:     count,
		resultsCh: nil,
	}
	return p, nil
}
