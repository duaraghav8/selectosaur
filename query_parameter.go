package main

type QueryParameter struct {
	Hostname           string
	StartTime, EndTime string
}

func (q *QueryParameter) HostID() int {
	return 0
}

// newQueryParam takes a string slice of 3 items:
// [hostname, start_time, end_time]
// It validates the data and returns a query param object in case of no errors.
func newQueryParam(rec []string) (*QueryParameter, error) {
	// validate all values
	return &QueryParameter{
		Hostname:  rec[0],
		StartTime: rec[1],
		EndTime:   rec[2],
	}, nil
}
