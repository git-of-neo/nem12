package main

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// newTestDB opens a fresh in-memory SQLite database, applies the schema, and
// registers a cleanup function that closes the connection when the test ends.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatalf("OpenDB(:memory:): %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := InitSchema(db); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	return db
}

// rowCount returns the number of rows currently in meter_readings.
func rowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM meter_readings`).Scan(&n); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	return n
}

// --------------------------------------------------------------------------
// InitSchema
// --------------------------------------------------------------------------

// TestInitSchema_CreatesTable verifies that the meter_readings table exists
// and has the expected columns after InitSchema is called.
func TestInitSchema_CreatesTable(t *testing.T) {
	db := newTestDB(t)

	// PRAGMA table_info returns one row per column; zero rows means the table
	// does not exist.
	rows, err := db.Query(`PRAGMA table_info(meter_readings)`)
	if err != nil {
		t.Fatalf("querying table_info: %v", err)
	}
	defer rows.Close()

	wantCols := map[string]bool{
		"id":          false,
		"nmi":         false,
		"timestamp":   false,
		"consumption": false,
	}
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			t.Fatalf("scanning table_info row: %v", err)
		}
		wantCols[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating table_info rows: %v", err)
	}

	for col, found := range wantCols {
		if !found {
			t.Errorf("column %q not found in meter_readings", col)
		}
	}
}

// TestInitSchema_Idempotent verifies that calling InitSchema a second time on a
// database that already has the table is a no-op and does not return an error.
func TestInitSchema_Idempotent(t *testing.T) {
	db := newTestDB(t)

	if err := InitSchema(db); err != nil {
		t.Errorf("InitSchema (second call): %v", err)
	}
}

// --------------------------------------------------------------------------
// WriteReadings
// --------------------------------------------------------------------------

// TestWriteReadings_Basic inserts a small slice of readings and confirms the
// exact number of rows lands in the database.
func TestWriteReadings_Basic(t *testing.T) {
	db := newTestDB(t)

	readings := []MeterReading{
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC), Consumption: 0.461},
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 30, 0, 0, time.UTC), Consumption: 0.810},
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 1, 0, 0, 0, time.UTC), Consumption: 0.568},
	}

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	if got := rowCount(t, db); got != len(readings) {
		t.Errorf("row count = %d, want %d", got, len(readings))
	}
}

// TestWriteReadings_Empty verifies that calling WriteReadings with an empty
// slice is a no-op that returns nil.
func TestWriteReadings_Empty(t *testing.T) {
	db := newTestDB(t)

	if err := WriteReadings(db, nil); err != nil {
		t.Errorf("WriteReadings(nil): %v", err)
	}
	if err := WriteReadings(db, []MeterReading{}); err != nil {
		t.Errorf("WriteReadings([]): %v", err)
	}

	if got := rowCount(t, db); got != 0 {
		t.Errorf("row count = %d, want 0", got)
	}
}

// TestWriteReadings_UniqueConstraint verifies that inserting the same (nmi,
// timestamp) pair twice does not return an error and results in only one row,
// honouring the ON CONFLICT DO NOTHING contract.
func TestWriteReadings_UniqueConstraint(t *testing.T) {
	db := newTestDB(t)

	r := MeterReading{
		NMI:         "NEM1201009",
		Timestamp:   time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC),
		Consumption: 1.0,
	}

	// Write the same reading twice in a single call.
	if err := WriteReadings(db, []MeterReading{r, r}); err != nil {
		t.Fatalf("WriteReadings with duplicate: %v", err)
	}

	if got := rowCount(t, db); got != 1 {
		t.Errorf("row count = %d, want 1 (duplicate should be silently ignored)", got)
	}
}

// TestWriteReadings_UniqueConstraint_TwoCalls verifies idempotency across
// separate WriteReadings invocations – re-importing the same data is safe.
func TestWriteReadings_UniqueConstraint_TwoCalls(t *testing.T) {
	db := newTestDB(t)

	readings := []MeterReading{
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC), Consumption: 0.461},
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 30, 0, 0, time.UTC), Consumption: 0.810},
	}

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("first WriteReadings: %v", err)
	}
	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("second WriteReadings (re-import): %v", err)
	}

	if got := rowCount(t, db); got != len(readings) {
		t.Errorf("row count = %d, want %d after re-import", got, len(readings))
	}
}

// TestWriteReadings_MultipleNMIs verifies that readings for different NMIs are
// all stored and that the nmi column is persisted correctly.
func TestWriteReadings_MultipleNMIs(t *testing.T) {
	db := newTestDB(t)

	ts := time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC)
	readings := []MeterReading{
		{NMI: "NEM1201009", Timestamp: ts, Consumption: 0.5},
		{NMI: "NEM1201010", Timestamp: ts, Consumption: 1.5}, // same ts, different NMI → not a duplicate
	}

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	if got := rowCount(t, db); got != 2 {
		t.Errorf("row count = %d, want 2", got)
	}

	// Confirm each NMI has exactly one row.
	for _, nmi := range []string{"NEM1201009", "NEM1201010"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM meter_readings WHERE nmi = ?`, nmi).Scan(&n); err != nil {
			t.Fatalf("counting rows for nmi %s: %v", nmi, err)
		}
		if n != 1 {
			t.Errorf("nmi %s: row count = %d, want 1", nmi, n)
		}
	}
}

// TestWriteReadings_StoredValues verifies that the nmi, timestamp, and
// consumption values are round-tripped through SQLite without corruption.
func TestWriteReadings_StoredValues(t *testing.T) {
	db := newTestDB(t)

	want := MeterReading{
		NMI:         "NEM1201009",
		Timestamp:   time.Date(2005, 3, 1, 6, 0, 0, 0, time.UTC),
		Consumption: 0.461,
	}

	if err := WriteReadings(db, []MeterReading{want}); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	var gotNMI, gotTS string
	var gotConsumption float64
	err := db.QueryRow(`SELECT nmi, timestamp, consumption FROM meter_readings`).
		Scan(&gotNMI, &gotTS, &gotConsumption)
	if err != nil {
		t.Fatalf("reading back row: %v", err)
	}

	if gotNMI != want.NMI {
		t.Errorf("nmi = %q, want %q", gotNMI, want.NMI)
	}
	if gotConsumption != want.Consumption {
		t.Errorf("consumption = %v, want %v", gotConsumption, want.Consumption)
	}

	// Timestamp is stored as RFC 3339; parse it back and compare.
	gotTime, err := time.Parse(time.RFC3339, gotTS)
	if err != nil {
		t.Fatalf("parsing stored timestamp %q: %v", gotTS, err)
	}
	if !gotTime.UTC().Equal(want.Timestamp.UTC()) {
		t.Errorf("timestamp = %v, want %v", gotTime.UTC(), want.Timestamp.UTC())
	}
}

// TestWriteReadings_UUIDPrimaryKey verifies that every inserted row receives a
// non-empty, unique UUID as its primary key.
func TestWriteReadings_UUIDPrimaryKey(t *testing.T) {
	db := newTestDB(t)

	readings := []MeterReading{
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC), Consumption: 0.1},
		{NMI: "NEM1201009", Timestamp: time.Date(2005, 3, 1, 0, 30, 0, 0, time.UTC), Consumption: 0.2},
	}

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	rows, err := db.Query(`SELECT id FROM meter_readings`)
	if err != nil {
		t.Fatalf("querying ids: %v", err)
	}
	defer rows.Close()

	seen := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning id: %v", err)
		}
		if id == "" {
			t.Error("id is empty string")
		}
		if seen[id] {
			t.Errorf("duplicate id %q", id)
		}
		seen[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating id rows: %v", err)
	}
}

// TestWriteReadings_FromSampleFile is an integration test that parses the
// embedded sample NEM12 file, converts all records to readings, and writes
// them to an in-memory database, confirming the final row count.
func TestWriteReadings_FromSampleFile(t *testing.T) {
	db := newTestDB(t)
	records := mustParse(t)
	readings := RecordsToReadings(records)

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	// Re-importing must be idempotent.
	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings (re-import): %v", err)
	}

	if got := rowCount(t, db); got != len(readings) {
		t.Errorf("persisted rows = %d, want %d", got, len(readings))
	}
}

// --------------------------------------------------------------------------
// RecordsToReadings
// --------------------------------------------------------------------------

// TestRecordsToReadings_Count verifies the total number of expanded readings
// from the sample file.
// The sample has 2 NMI blocks × 4 days × 48 intervals (30-min) = 384 readings.
func TestRecordsToReadings_Count(t *testing.T) {
	records := mustParse(t)
	readings := RecordsToReadings(records)

	const want = 384 // 2 NMIs × 4 days × 48 intervals
	if len(readings) != want {
		t.Errorf("RecordsToReadings() = %d readings, want %d", len(readings), want)
	}
}

// TestRecordsToReadings_NMIAssignment verifies that each reading carries the
// NMI from the enclosing 200 record, not the next one.
func TestRecordsToReadings_NMIAssignment(t *testing.T) {
	records := mustParse(t)
	readings := RecordsToReadings(records)

	// First 192 readings (4 days × 48) belong to NEM1201009.
	for i := 0; i < 192; i++ {
		if readings[i].NMI != "NEM1201009" {
			t.Errorf("readings[%d].NMI = %q, want %q", i, readings[i].NMI, "NEM1201009")
			break
		}
	}

	// Next 192 readings belong to NEM1201010.
	for i := 192; i < 384; i++ {
		if readings[i].NMI != "NEM1201010" {
			t.Errorf("readings[%d].NMI = %q, want %q", i, readings[i].NMI, "NEM1201010")
			break
		}
	}
}

// TestRecordsToReadings_TimestampExpansion verifies that interval timestamps
// are correctly calculated from the interval date plus the interval index
// multiplied by the interval length.
func TestRecordsToReadings_TimestampExpansion(t *testing.T) {
	records := mustParse(t)
	readings := RecordsToReadings(records)

	// Interval 0 → midnight on 2005-03-01.
	wantFirst := time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC)
	if !readings[0].Timestamp.Equal(wantFirst) {
		t.Errorf("readings[0].Timestamp = %v, want %v", readings[0].Timestamp, wantFirst)
	}

	// Interval 1 → 00:30 on 2005-03-01 (30-minute intervals).
	wantSecond := wantFirst.Add(30 * time.Minute)
	if !readings[1].Timestamp.Equal(wantSecond) {
		t.Errorf("readings[1].Timestamp = %v, want %v", readings[1].Timestamp, wantSecond)
	}

	// Interval 47 → 23:30 on 2005-03-01 (last interval of the first day).
	wantLast := time.Date(2005, 3, 1, 23, 30, 0, 0, time.UTC)
	if !readings[47].Timestamp.Equal(wantLast) {
		t.Errorf("readings[47].Timestamp = %v, want %v", readings[47].Timestamp, wantLast)
	}
}

// TestRecordsToReadings_ConsumptionValues spot-checks that consumption values
// from the parsed IntervalDataRecord are preserved in the expanded readings.
func TestRecordsToReadings_ConsumptionValues(t *testing.T) {
	records := mustParse(t)
	readings := RecordsToReadings(records)

	cases := []struct {
		index int
		want  float64
	}{
		{12, 0.461}, // first non-zero interval (06:00)
		{13, 0.810},
		{14, 0.568},
		{15, 1.234},
		{46, 0.345},
		{47, 0.231},
	}

	for _, tc := range cases {
		if readings[tc.index].Consumption != tc.want {
			t.Errorf("readings[%d].Consumption = %v, want %v",
				tc.index, readings[tc.index].Consumption, tc.want)
		}
	}
}

// TestRecordsToReadings_Empty verifies that an empty (or header-only) record
// slice produces no readings without panicking.
func TestRecordsToReadings_Empty(t *testing.T) {
	if got := RecordsToReadings(nil); len(got) != 0 {
		t.Errorf("RecordsToReadings(nil) = %d readings, want 0", len(got))
	}
	if got := RecordsToReadings([]Record{}); len(got) != 0 {
		t.Errorf("RecordsToReadings([]) = %d readings, want 0", len(got))
	}
}

// TestRecordsToReadings_CustomInterval verifies that a 15-minute interval block
// generates 96 readings per day rather than 48.
func TestRecordsToReadings_CustomInterval(t *testing.T) {
	// 15-minute intervals → 96 periods per day.
	const intervalLen = 15
	const numIntervals = 24 * 60 / intervalLen // 96

	values := make([]float64, numIntervals)
	for i := range values {
		values[i] = float64(i) * 0.1
	}

	records := []Record{
		NMIDataDetailsRecord{NMI: "TEST000001", IntervalLength: intervalLen},
		IntervalDataRecord{
			IntervalDate:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			IntervalValues: values,
		},
	}

	readings := RecordsToReadings(records)
	if len(readings) != numIntervals {
		t.Fatalf("readings count = %d, want %d", len(readings), numIntervals)
	}

	// Interval 1 should start 15 minutes after midnight.
	want := time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC)
	if !readings[1].Timestamp.Equal(want) {
		t.Errorf("readings[1].Timestamp = %v, want %v", readings[1].Timestamp, want)
	}
}

// TestRecordsToReadings_WrittenToMemoryDB is a combined test: expand records
// and immediately write them to an in-memory database, then verify counts and
// a spot-checked value to confirm the full pipeline end-to-end.
func TestRecordsToReadings_WrittenToMemoryDB(t *testing.T) {
	db := newTestDB(t)

	input := strings.Join([]string{
		"100,NEM12,200506081149,UNITEDDP,NEMMCO",
		"200,NEM1201009,E1E2,1,E1,,NEM12DP1,kWh,30,20050310",
		"300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204",
		"900",
	}, "\n")

	records, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	readings := RecordsToReadings(records)
	const wantReadings = 48
	if len(readings) != wantReadings {
		t.Fatalf("RecordsToReadings() = %d, want %d", len(readings), wantReadings)
	}

	if err := WriteReadings(db, readings); err != nil {
		t.Fatalf("WriteReadings: %v", err)
	}

	if got := rowCount(t, db); got != wantReadings {
		t.Errorf("row count = %d, want %d", got, wantReadings)
	}

	// Spot-check: interval 12 (06:00) should have consumption 0.461.
	var consumption float64
	err = db.QueryRow(
		`SELECT consumption FROM meter_readings WHERE nmi = ? AND timestamp = ?`,
		"NEM1201009",
		time.Date(2005, 3, 1, 6, 0, 0, 0, time.UTC).UTC().Format(time.RFC3339),
	).Scan(&consumption)
	if err != nil {
		t.Fatalf("querying spot-check row: %v", err)
	}
	if consumption != 0.461 {
		t.Errorf("spot-check consumption = %v, want 0.461", consumption)
	}
}
