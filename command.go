package main

import (
	"encoding/csv"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"math"
	"os"
	"time"
)

var command = &cobra.Command{
	Use:     "selectosaur --qp FILE --worker-count COUNT",
	Short:   "Analyze TimescaleDB query performance",
	RunE:    commandHandler,
	Example: "selectosaur --qp /tmp/query_params.csv --worker-count 4",
	Long:    ``,
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

func commandHandler(cmd *cobra.Command, args []string) error {
	// TODO: create a thread-safe datastore object to create sql queries & interact with timescaledb

	wc, _ := cmd.Flags().GetInt("worker-count")
	wp, err := newWorkerPool(wc)
	if err != nil {
		return fmt.Errorf("failed to create worker pool: %v", err)
	}

	qpFile, _ := cmd.Flags().GetString("qp")
	f, err := os.Open(qpFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", qpFile, err)
	}
	defer f.Close()

	var (
		elapsed time.Duration
		minQ    time.Duration = math.MaxInt
		maxQ    time.Duration = -1
	)
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

	for i := 0; i < queryCount; i++ {
		res := <-wp.Results
		elapsed += res.ElapsedTime

		if res.ElapsedTime > maxQ {
			maxQ = res.ElapsedTime
		}
		if res.ElapsedTime < minQ {
			minQ = res.ElapsedTime
		}
	}

	fmt.Printf("Total number of queries run:   %d\n", queryCount)
	fmt.Printf("Total time across all queries: %f seconds\n", elapsed.Seconds())

	avg := elapsed.Seconds() / float64(queryCount)
	fmt.Printf("Average query time:            %f seconds\n", avg)

	fmt.Printf("Minimum query time:            %d milliseconds\n", minQ.Milliseconds())
	fmt.Printf("Maximum query time:            %d milliseconds\n", maxQ.Milliseconds())
	// TODO: median query time

	return nil
}
