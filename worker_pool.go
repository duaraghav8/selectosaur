package main

import (
	"errors"
	"fmt"
	"time"
)

type Worker struct {
	JobQ chan *QueryParameter
}

type Result struct {
	ElapsedTime time.Duration
}

type WorkerPool struct {
	Results chan *Result
	workers []*Worker
}

func (w *Worker) Start() {
	for qp := range w.JobQ {
		fmt.Println("got job", qp)
		time.Sleep(time.Millisecond * 500)
	}
}

func (wp *WorkerPool) Submit(qp *QueryParameter) {
	// Determine target worker ID using modulus operation.
	// This guarantees that queries of the same host go to the same worker each time.
	wid := qp.HostID() % len(wp.workers)
	wp.workers[wid].JobQ <- qp
}

func newWorkerPool(count int) (*WorkerPool, error) {
	if count < 1 {
		return nil, errors.New("worker count cannot be less than 1")
	}
	// TODO: wc upper limit?

	workers := make([]*Worker, count, count)
	for i := 0; i < count; i++ {
		workers[i] = &Worker{}
		workers[i].Start()
	}

	p := &WorkerPool{
		workers: workers,
		Results: make(chan *Result, count),
	}
	return p, nil
}
