package main

import (
	"fmt"
	"hash/fnv"
)

type QueryParameter struct {
	Hostname           string
	HostID             int
	StartTime, EndTime string
}

// newQueryParam takes a string slice of 3 items:
// [hostname, start_time, end_time]
// It returns a query param object which is also assigned a host ID based on hostname.
// The ID is determined using the 32-bit FNV-1a Hashing scheme to ensure that the
// hash value of a specific hostname is always the same.
func newQueryParam(rec []string) (*QueryParameter, error) {
	res := &QueryParameter{
		Hostname: rec[0], StartTime: rec[1], EndTime: rec[2],
	}

	h := fnv.New32a()
	if _, err := h.Write([]byte(res.Hostname)); err != nil {
		return nil, fmt.Errorf("failed to generate ID for host %s: %v", res.Hostname, err)
	}
	res.HostID = int(h.Sum32())

	return res, nil
}
