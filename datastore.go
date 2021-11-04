package main

import (
	"github.com/jackc/pgx/v4/pgxpool"
)

// Datastore interacts with a Timescale database
type Datastore struct {
	conn *pgxpool.Pool
}
