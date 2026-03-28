package main

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// dbWorkerPoolSize controls the maximum number of goroutines that may issue
// concurrent INSERT statements against SQLite. Keeping this small prevents
// write contention from overwhelming the database.
const dbWorkerPoolSize = 4

// MeterReading is a single interval reading ready to be persisted.
type MeterReading struct {
	NMI         string
	Timestamp   time.Time
	Consumption float64
}

// OpenDB opens (or creates) a SQLite database at dsn and configures it for
// concurrent use. Pass ":memory:" for a transient in-memory database.
//
// In-memory SQLite databases are scoped to a single connection — every new
// connection gets a fresh, empty database, so schema created on one connection
// would be invisible to all others. When an in-memory DSN is detected the pool
// is pinned to a single connection so the entire application shares the same
// in-memory store. WAL journal mode is also incompatible with :memory: and is
// therefore only applied to file-backed databases.
//
// For file-backed databases the connection pool is capped to dbWorkerPoolSize
// so goroutines never hold more connections than the writer pool can use.
func OpenDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite db: %w", err)
	}

	isMemory := dsn == ":memory:" || strings.Contains(dsn, "mode=memory")

	if !isMemory {
		// WAL mode gives better concurrent read/write throughput on disk.
		if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
			db.Close()
			return nil, fmt.Errorf("setting WAL journal mode: %w", err)
		}
	}

	// Give waiting writers up to 5 s before returning SQLITE_BUSY.
	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("setting busy_timeout: %w", err)
	}

	if isMemory {
		// Pin to one connection so every caller sees the same in-memory DB.
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	} else {
		// Cap the pool to match the writer pool size.
		db.SetMaxOpenConns(dbWorkerPoolSize)
	}

	return db, nil
}

// InitSchema creates the meter_readings table when it does not already exist.
// The schema is adapted from the PostgreSQL DDL: UUIDs are stored as TEXT and
// timestamps as RFC 3339 strings, which SQLite handles via TEXT affinity.
// Calling InitSchema on a database that already has the table is a no-op.
func InitSchema(db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS meter_readings (
    id          TEXT NOT NULL,
    nmi         TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    consumption REAL NOT NULL,
    CONSTRAINT meter_readings_pk             PRIMARY KEY (id),
    CONSTRAINT meter_readings_unique_reading UNIQUE      (nmi, timestamp)
);`
	if _, err := db.Exec(ddl); err != nil {
		return fmt.Errorf("creating meter_readings table: %w", err)
	}
	return nil
}

// RecordsToReadings walks a slice of parsed NEM12 records and expands every
// IntervalDataRecord into one MeterReading per interval period. The NMI and
// interval length are carried forward from the most recently seen
// NMIDataDetailsRecord, exactly as a conforming NEM12 reader must do.
func RecordsToReadings(records []Record) []MeterReading {
	var (
		readings       []MeterReading
		currentNMI     string
		intervalLength = 30 // default; overwritten by each 200 record
	)

	for _, rec := range records {
		switch v := rec.(type) {
		case NMIDataDetailsRecord:
			currentNMI = v.NMI
			intervalLength = v.IntervalLength

		case IntervalDataRecord:
			for i, val := range v.IntervalValues {
				ts := v.IntervalDate.Add(time.Duration(i*intervalLength) * time.Minute)
				readings = append(readings, MeterReading{
					NMI:         currentNMI,
					Timestamp:   ts,
					Consumption: val,
				})
			}
		}
	}

	return readings
}

// WriteReadings persists all readings to the database using a bounded worker
// pool of dbWorkerPoolSize goroutines. Each goroutine draws work from a shared
// jobs channel and issues individual INSERT statements. Duplicate rows
// (same nmi + timestamp) are silently ignored via ON CONFLICT DO NOTHING.
//
// The first insertion error encountered by any worker is returned; subsequent
// errors are discarded to keep the implementation lock-free. A nil return means
// every row was written (or gracefully skipped as a duplicate).
func WriteReadings(db *sql.DB, readings []MeterReading) error {
	jobs := make(chan MeterReading)

	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)

	// Start the fixed-size worker pool.
	for range dbWorkerPoolSize {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range jobs {
				if err := insertReading(db, r); err != nil {
					errOnce.Do(func() { firstErr = err })
				}
			}
		}()
	}

	// Feed every reading into the pool, then signal completion.
	for _, r := range readings {
		jobs <- r
	}
	close(jobs)

	wg.Wait()
	return firstErr
}

// insertReading inserts a single MeterReading into meter_readings.
// A fresh UUID v4 is generated in Go for each row. Conflicts on (nmi, timestamp)
// are silently ignored so that re-importing a file is always safe.
func insertReading(db *sql.DB, r MeterReading) error {
	const q = `
INSERT INTO meter_readings (id, nmi, timestamp, consumption)
VALUES (?, ?, ?, ?)
ON CONFLICT (nmi, timestamp) DO NOTHING;`

	id := uuid.New().String()
	ts := r.Timestamp.UTC().Format(time.RFC3339)

	if _, err := db.Exec(q, id, r.NMI, ts, r.Consumption); err != nil {
		return fmt.Errorf("inserting reading (nmi=%s ts=%s): %w", r.NMI, ts, err)
	}
	return nil
}
