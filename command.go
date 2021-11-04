package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/montanaflynn/stats"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strings"
)

var command = &cobra.Command{
	Use:     "selectosaur --qp FILE --worker-count COUNT",
	Short:   "Analyze TimescaleDB query performance",
	RunE:    commandHandler,
	Example: "selectosaur --qp /tmp/query_params.csv --worker-count 4",
	Long: `    The DB_CONNECTION_STRING environment variable must be supplied. For example:
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
	fmt.Printf("Total number of queries run:   %d\n", len(latencies))

	fmt.Printf("Number of failures:            %d", len(failures))

	elapsed, err := stats.Sum(latencies)
	if err != nil {
		return fmt.Errorf("failed to calculate total query time: %v", err)
	}
	fmt.Printf("Total time across all queries: %f milliseconds\n", elapsed)

	avg, err := stats.Mean(latencies)
	if err != nil {
		return fmt.Errorf("failed to calculate average query time: %v", err)
	}
	fmt.Printf("Average query time:            %f seconds\n", avg)

	min, err := stats.Min(latencies)
	if err != nil {
		return fmt.Errorf("failed to determine minimum query time: %v", err)
	}
	fmt.Printf("Minimum query time:            %f milliseconds\n", min)

	max, err := stats.Max(latencies)
	if err != nil {
		return fmt.Errorf("failed to determine maximum query time: %v", err)
	}
	fmt.Printf("Maximum query time:            %f milliseconds\n", max)

	med, err := stats.Median(latencies)
	if err != nil {
		return fmt.Errorf("failed to median query time: %v", err)
	}
	fmt.Printf("Median query time:             %f milliseconds\n", med)

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

	// create worker pool to execute jobs
	wc, _ := cmd.Flags().GetInt("worker-count")
	wp, err := newWorkerPool(wc, &Datastore{dbPool})
	if err != nil {
		return fmt.Errorf("failed to create worker pool: %v", err)
	}

	// read & submit query parameters to the worker pool
	qpFile, _ := cmd.Flags().GetString("qp")
	f, err := os.Open(qpFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", qpFile, err)
	}
	defer f.Close()

	var queryCount int

	reader := csv.NewReader(f)
	reader.Read() // Skip the first row that contains field names
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			// no more query parameters left
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %v", err)
		}

		queryCount++
		qp, err := newQueryParam(rec)
		if err != nil {
			return fmt.Errorf("failed to parse query param CSV record %v: %v", rec, err)
		}
		wp.Submit(qp)
	}

	if queryCount == 0 {
		return errors.New("there are no queries to run")
	}

	// prepare final stats report
	latencies := make([]float64, 0, queryCount) // query latencies in ms
	failures := make([]error, 0, queryCount)

	for i := 0; i < queryCount; i++ {
		res := <-wp.ResultsCh()
		if res.Err != nil {
			// optionally print the failure message, leaving that out for now
			failures = append(failures, res.Err)
			continue
		}
		latencies = append(latencies, float64(res.ElapsedTime.Milliseconds()))
	}

	return report(latencies, failures)
}
