package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
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
	wc, _ := cmd.Flags().GetInt("worker-count")
	if wc < 1 {
		return errors.New("worker count cannot be less than 1")
	}
	// TODO: wc upper limit?

	qpFile, _ := cmd.Flags().GetString("qp")
	f, err := os.Open(qpFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", qpFile, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	// TODO: ensure that we skip first row (which contains field names)
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			// no more query parameters left
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record: %v", err)
		}

		qp, err := newQueryParam(rec)
		if err != nil {
			return fmt.Errorf("failed to parse query param CSV record %v: %v", rec, err)
		}

		fmt.Println(qp)
	}

	// create a thread-safe datastore object to create sql queries & interact with timescaledb
	// create worker pool based on count
	// load query params 1-by-1 (stream)
	//
	// for each qp:
	//   get host's id from hostname & determine its worker (using modulo)
	//   submit the qp to target worker (async, non-blocking)
	//
	// collect all worker responses
	// calculate total no. of queries, total time across all queries
	// calculate query times: min, med, avg, max

	return nil
}
