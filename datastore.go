package main

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
)

// TODO: fix the seconds offset problem
const timeSlicedCpuStatsQuery = `EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
   time_bucket('1 minute', ts) AS clock, MAX(usage), MIN(usage)
FROM cpu_usage
WHERE
   host = $1 AND ts BETWEEN $2 AND $3
GROUP BY clock`

// explainResult contains the response from an EXPLAIN ANALYZE query
// run in timescale db.
type explainResult struct {
	PlanTimeMs float64 `json:"Planning Time"`
	ExecTimeMs float64 `json:"Execution Time"`
}

// Datastore interacts with a Timescale database.
// It is thread-safe and is designed to be called by multiple goroutines
// concurrently.
type Datastore struct {
	connPool *pgxpool.Pool
}

// CPUStatsQueryExecTime returns the total processing time for the query
// that computes cpu stats at 1-min intervals for the given query param.
// Processing time here is the sum of Planning time & Execution time
// returned by EXPLAIN ANALYZE for the query
// (see https://www.postgresql.org/docs/9.4/using-explain.html).
func (d *Datastore) CPUStatsQueryExecTime(ctx context.Context, qp *QueryParameter) (float64, error) {
	var res []explainResult
	row := d.connPool.QueryRow(ctx, timeSlicedCpuStatsQuery, qp.Hostname, qp.StartTime, qp.EndTime)
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res[0].ExecTimeMs + res[0].PlanTimeMs, nil
}
