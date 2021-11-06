package main

import (
	"fmt"
	"time"
)

const maxWorkers = 10000

type Worker struct {
	id       int
	jobCh    chan *QueryParameter
	resultsQ chan *Result
}

func (w *Worker) Start() {
	for qp := range w.jobCh {
		// TODO
		fmt.Printf("%d executing job: %v\n", w.id, qp)
		time.Sleep(time.Millisecond * 500)
		r := &Result{
			Job:         qp,
			Err:         nil,
			ElapsedTime: time.Millisecond * 500,
		}
		w.resultsQ <- r
	}
}

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
	db      *Datastore
	count   int
	workers []*Worker
	jobsQ   chan *QueryParameter
}

func (wp *WorkerPool) Close() {
	// close job queue (or not)
	// close all workers' job channels so they can exit
	for _, w := range wp.workers {
		close(w.jobCh)
	}
}

func (wp *WorkerPool) start() {
	for qp := range wp.jobsQ {
		// map the query parameter to the right worker
		wid := qp.HostID() % wp.count
		wp.workers[wid].jobCh <- qp
	}
}

func newWorkerPool(
	count int, db *Datastore, jobsQ chan *QueryParameter, resultsQ chan *Result,
) (*WorkerPool, error) {
	if count < 1 || count > maxWorkers {
		return nil, fmt.Errorf("worker count should be between 1 and %d", maxWorkers)
	}

	w := make([]*Worker, count, count)
	for i := 0; i < count; i++ {
		w[i] = &Worker{
			id:       i,
			jobCh:    make(chan *QueryParameter),
			resultsQ: resultsQ,
		}
		go w[i].Start()
	}

	p := &WorkerPool{db: db, count: count, jobsQ: jobsQ, workers: w}
	go p.start()

	return p, nil
}
