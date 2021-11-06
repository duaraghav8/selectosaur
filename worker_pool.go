package main

import (
	"context"
	"fmt"
)

const maxWorkers = 10000

// Result contains the net output of a job executed by a Worker.
type Result struct {
	Job        *QueryParameter
	Err        error
	ExecTimeMs float64
}

type Worker struct {
	id       int
	jobCh    chan *QueryParameter
	resultsQ chan *Result
	db       *Datastore
}

func (w *Worker) Start(ctx context.Context) {
	for qp := range w.jobCh {
		t, err := w.db.CPUStatsQueryExecTime(ctx, qp)
		r := &Result{
			Job: qp, Err: err, ExecTimeMs: t,
		}
		w.resultsQ <- r
	}
}

// WorkerPool manages a pool of workers to perform multiple timescale query
//execution jobs concurrently.
// It guarantees that for every job submitted, there will be exactly 1 Result
// returned via its results channel.
type WorkerPool struct {
	count   int
	workers []*Worker
	jobsQ   chan *QueryParameter
}

// Close ensures all workers in the pool exit and free up all resources.
// This method must be called after closing the Job queue channel of
// the pool.
func (wp *WorkerPool) Close() {
	// close all workers' job channels so they can exit
	for _, w := range wp.workers {
		close(w.jobCh)
	}
}

func (wp *WorkerPool) start() {
	for qp := range wp.jobsQ {
		// map the query parameter to the right worker
		wid := qp.HostID % wp.count
		wp.workers[wid].jobCh <- qp
	}
}

func newWorkerPool(
	ctx context.Context,
	count int,
	db *Datastore,
	jobsQ chan *QueryParameter,
	resultsQ chan *Result,
) (*WorkerPool, error) {
	if count < 1 || count > maxWorkers {
		return nil, fmt.Errorf("worker count should be between 1 and %d", maxWorkers)
	}

	w := make([]*Worker, count, count)
	for i := 0; i < count; i++ {
		w[i] = &Worker{
			id:       i,
			db:       db,
			jobCh:    make(chan *QueryParameter),
			resultsQ: resultsQ,
		}
		go w[i].Start(ctx)
	}

	p := &WorkerPool{count: count, jobsQ: jobsQ, workers: w}
	go p.start()

	return p, nil
}
