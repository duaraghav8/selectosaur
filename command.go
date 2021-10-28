package main

import (
	"fmt"
	"github.com/spf13/cobra"
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
	qpf, _ := cmd.Flags().GetString("qp")
	wc, _ := cmd.Flags().GetInt("worker-count")

	// create a thread-safe datastore object to create sql queries & interact with timescaledb
	// validations for qpf & wc
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

	fmt.Println(qpf)
	fmt.Println(wc)
	return nil
}
