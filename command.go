package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/montanaflynn/stats"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var command = &cobra.Command{
	Use:     "selectosaur --qp FILE --worker-count COUNT",
	Short:   "Analyze TimescaleDB query performance",
	RunE:    commandHandler,
	Example: "selectosaur --qp /tmp/query_params.csv --worker-count 4",
	Long: `    Selectosaur runs SQL queries on Timescale DB based on
    user-supplied parameters and outputs stats for them.

    The DB_CONNECTION_STRING environment variable must be set. For example:
    postgres://user:password@host:31703/dbname?sslmode=require`,
}

func init() {
	// Prevent error message showing up twice
	command.SilenceErrors = true
	// Prevent usage from showing up when the command logic returns an error
	command.SilenceUsage = true

	command.Flags().String("qp", "", "Exact path to the CSV file containing query params")
	_ = command.MarkFlagRequired("qp")

	command.Flags().Int("worker-count", 1, "Number of workers")
}

// report generates and prints the final stats for query latencies & failures
func report(latencies []float64, failures []error) error {
	fmt.Printf("Total number of queries run:      %d\n", len(latencies)+len(failures))

	fmt.Printf("Number of failures:               %d\n", len(failures))

	if len(latencies) == 0 {
		return errors.New("all queries failed, no stats to calculate")
	}

	elapsed, err := stats.Sum(latencies)
	if err != nil {
		return fmt.Errorf("failed to calculate total query time: %v", err)
	}
	fmt.Printf("Total time across all queries:    %f milliseconds\n", elapsed)

	avg, err := stats.Mean(latencies)
	if err != nil {
		return fmt.Errorf("failed to calculate average query time: %v", err)
	}
	fmt.Printf("Average query time:               %f milliseconds\n", avg)

	min, err := stats.Min(latencies)
	if err != nil {
		return fmt.Errorf("failed to determine minimum query time: %v", err)
	}
	fmt.Printf("Minimum query time:               %f milliseconds\n", min)

	max, err := stats.Max(latencies)
	if err != nil {
		return fmt.Errorf("failed to determine maximum query time: %v", err)
	}
	fmt.Printf("Maximum query time:               %f milliseconds\n", max)

	med, err := stats.Median(latencies)
	if err != nil {
		return fmt.Errorf("failed to median query time: %v", err)
	}
	fmt.Printf("Median query time:                %f milliseconds\n", med)

	return nil
}

func commandHandler(cmd *cobra.Command, args []string) error {
	// create a connection pool to Timescale DB
	connStr := os.Getenv("DB_CONNECTION_STRING")
	if strings.TrimSpace(connStr) == "" {
		return errors.New("DB_CONNECTION_STRING environment variable not supplied")
	}

	dbPool, err := pgxpool.Connect(cmd.Context(), connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to timescale database: %v", err)
	}
	defer dbPool.Close()

	// read in CSV records
	qpFile, _ := cmd.Flags().GetString("qp")
	f, err := os.Open(qpFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", qpFile, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.Read() // Skip the first row because it contains headers

	records, err := reader.ReadAll()
	if len(records) == 0 {
		return errors.New("there are no queries to run")
	}

	// create worker pool to execute jobs
	jobsQ := make(chan *QueryParameter, len(records))
	resultsQ := make(chan *Result, len(records))
	wc, _ := cmd.Flags().GetInt("worker-count")

	pool, err := newWorkerPool(wc, &Datastore{dbPool}, jobsQ, resultsQ)
	if err != nil {
		return fmt.Errorf("failed to create worker pool: %v", err)
	}
	defer pool.Close()

	// submit query parameters as jobs to the pool
	for _, rec := range records {
		qp, err := newQueryParam(rec)
		if err != nil {
			return fmt.Errorf("failed to parse query param CSV record %v: %v", rec, err)
		}
		jobsQ <- qp
	}
	close(jobsQ)

	// prepare final stats report
	latencies := make([]float64, 0, len(records)) // query latencies in ms
	failures := make([]error, 0, len(records))

	for i := 0; i < len(records); i++ {
		res := <-resultsQ
		if res.Err != nil {
			// optionally print the failure message, leaving that out for now
			failures = append(failures, res.Err)
			continue
		}
		latencies = append(latencies, float64(res.ElapsedTime.Milliseconds()))
	}

	return report(latencies, failures)
}
