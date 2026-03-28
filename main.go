package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// RecordType represents the NEM12 record indicator as an integer.
type RecordType int

const (
	RecordTypeHeader         RecordType = 100
	RecordTypeNMIDataDetails RecordType = 200
	RecordTypeIntervalData   RecordType = 300
	RecordTypeIntervalEvent  RecordType = 400
	RecordTypeB2BDetails     RecordType = 500
	RecordTypeEndOfData      RecordType = 900
)

// Record is the interface implemented by all NEM12 record types.
type Record interface {
	Type() RecordType
}

// HeaderRecord represents a NEM12 100 (Header) record.
type HeaderRecord struct {
	VersionHeader   string    // Must be "NEM12"
	DateTime        time.Time // File creation datetime (yyyyMMddHHmm)
	FromParticipant string    // Sending participant identifier
	ToParticipant   string    // Receiving participant identifier
}

func (r HeaderRecord) Type() RecordType { return RecordTypeHeader }

// NMIDataDetailsRecord represents a NEM12 200 record containing NMI metadata.
// It introduces a block of 300 records that all belong to this NMI.
type NMIDataDetailsRecord struct {
	NMI                   string    // National Metering Identifier (field 2)
	NMIConfiguration      string    // NMI configuration, e.g. "E1E2" (field 3)
	RegisterID            string    // Register identifier (field 4)
	NMISuffix             string    // NMI data stream suffix (field 5)
	MDMDataStreamID       string    // MDM data stream identifier, optional (field 6)
	MeterSerialNumber     string    // Meter serial number (field 7)
	UOM                   string    // Unit of measure, e.g. "kWh" (field 8)
	IntervalLength        int       // Interval length in minutes, e.g. 30 (field 9)
	NextScheduledReadDate time.Time // Date of next scheduled meter read (field 10)
}

func (r NMIDataDetailsRecord) Type() RecordType { return RecordTypeNMIDataDetails }

// IntervalDataRecord represents a NEM12 300 record containing consumption data
// for a single calendar day. There are 24*60/IntervalLength interval values,
// one per interval period.
type IntervalDataRecord struct {
	IntervalDate      time.Time // Date the intervals cover (field 2)
	IntervalValues    []float64 // Consumption for each interval period (fields 3–N)
	QualityMethod     string    // Quality method code, e.g. "A" for Actual
	ReasonCode        string    // Reason code for quality substitution (optional)
	ReasonDescription string    // Human-readable reason (optional)
	UpdateDateTime    time.Time // When this record was last updated (yyyyMMddHHmmss)
	MSATSLoadDateTime time.Time // When this record was loaded into MSATS (yyyyMMddHHmmss)
}

func (r IntervalDataRecord) Type() RecordType { return RecordTypeIntervalData }

// IntervalEventRecord represents a NEM12 400 record describing a quality event
// that applies to a range of intervals within the preceding 300 record.
type IntervalEventRecord struct {
	StartInterval     int    // First interval affected (inclusive)
	EndInterval       int    // Last interval affected (inclusive)
	QualityMethod     string // Quality method code
	ReasonCode        string // Reason code (optional)
	ReasonDescription string // Reason description (optional)
}

func (r IntervalEventRecord) Type() RecordType { return RecordTypeIntervalEvent }

// B2BDetailsRecord represents a NEM12 500 record containing B2B transaction details
// associated with the preceding 200 record's NMI block.
type B2BDetailsRecord struct {
	TransCode         string    // Transaction code
	RetServiceOrder   string    // Retailer service order number
	ReadDateTime      time.Time // Meter read datetime (yyyyMMddHHmmss)
	IsMeteredCustomer bool      // "Y" → true, anything else → false
}

func (r B2BDetailsRecord) Type() RecordType { return RecordTypeB2BDetails }

// EndOfDataRecord represents a NEM12 900 record that marks the end of the file.
type EndOfDataRecord struct{}

func (r EndOfDataRecord) Type() RecordType { return RecordTypeEndOfData }

// knownRecordTypes is the set of valid NEM12 record-type identifier strings.
// It is used by Parser to detect the start of a new logical record.
var knownRecordTypes = map[string]bool{
	"100": true,
	"200": true,
	"300": true,
	"400": true,
	"500": true,
	"900": true,
}

// --------------------------------------------------------------------------
// Streaming parser
// --------------------------------------------------------------------------

// Parser reads a NEM12 stream and yields one Record at a time via Next.
//
// Physical line continuation (where a logical record is split across multiple
// physical lines) is handled transparently: continuation lines are stitched
// onto the current logical line until a new record-type prefix is seen.
//
// Memory usage is O(1) with respect to file size — only the current physical
// line and the logical record under construction are live at any point.
type Parser struct {
	scanner        *bufio.Scanner
	current        strings.Builder
	intervalLength int  // carried forward from the most recent 200 record
	exhausted      bool // true once the scanner has reached EOF or an error
}

// NewParser returns a Parser that reads NEM12 data from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{scanner: bufio.NewScanner(r)}
}

// Next returns the next parsed Record from the stream.
// It returns (nil, io.EOF) when the file is fully consumed.
// Any other non-nil error indicates an I/O or parse failure.
func (p *Parser) Next() (Record, error) {
	for !p.exhausted {
		if !p.scanner.Scan() {
			p.exhausted = true
			if err := p.scanner.Err(); err != nil {
				return nil, err
			}
			break
		}

		line := strings.TrimSpace(p.scanner.Text())
		if line == "" {
			continue
		}

		prefix, _, _ := strings.Cut(line, ",")
		if knownRecordTypes[prefix] && p.current.Len() > 0 {
			// The previous logical record is now complete. Parse it, stash the
			// opening line of the next record in current, and return.
			logical := p.current.String()
			p.current.Reset()
			p.current.WriteString(line)
			return p.parseLogicalLine(logical)
		}

		// Either this is the very first line, or it is a continuation line
		// that belongs to the record already accumulating in current.
		p.current.WriteString(line)
	}

	// Flush the final logical record once the scanner is exhausted.
	if p.current.Len() > 0 {
		logical := p.current.String()
		p.current.Reset()
		return p.parseLogicalLine(logical)
	}

	return nil, io.EOF
}

// parseLogicalLine parses a single complete logical line and returns the
// corresponding Record. It updates p.intervalLength whenever a 200 record is
// seen so that subsequent 300 records know how many interval values to expect.
func (p *Parser) parseLogicalLine(line string) (Record, error) {
	typeStr, _, _ := strings.Cut(line, ",")

	switch typeStr {
	case "100":
		rec, err := parseHeaderRecord(line)
		if err != nil {
			return nil, fmt.Errorf("100 record: %w", err)
		}
		return rec, nil

	case "200":
		rec, err := parseNMIDataDetailsRecord(line)
		if err != nil {
			return nil, fmt.Errorf("200 record: %w", err)
		}
		p.intervalLength = rec.IntervalLength
		return rec, nil

	case "300":
		rec, err := parseIntervalDataRecord(line, p.intervalLength)
		if err != nil {
			return nil, fmt.Errorf("300 record: %w", err)
		}
		return rec, nil

	case "400":
		rec, err := parseIntervalEventRecord(line)
		if err != nil {
			return nil, fmt.Errorf("400 record: %w", err)
		}
		return rec, nil

	case "500":
		rec, err := parseB2BDetailsRecord(line)
		if err != nil {
			return nil, fmt.Errorf("500 record: %w", err)
		}
		return rec, nil

	case "900":
		return EndOfDataRecord{}, nil

	default:
		return nil, fmt.Errorf("unknown record type %q", typeStr)
	}
}

// --------------------------------------------------------------------------
// Convenience batch parser (used by tests)
// --------------------------------------------------------------------------

// Parse reads a complete NEM12 file from r and returns all records in file
// order. It is a thin wrapper around Parser.Next() that collects every record
// into a slice.
//
// For large files where memory is a concern, use NewParser and call Next() in
// a loop so that only one record is live in memory at a time.
func Parse(r io.Reader) ([]Record, error) {
	p := NewParser(r)
	var records []Record
	for {
		rec, err := p.Next()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("reading NEM12 input: %w", err)
		}
		records = append(records, rec)
	}
}

// --------------------------------------------------------------------------
// Individual record parsers
// --------------------------------------------------------------------------

// parseHeaderRecord parses a NEM12 100 record.
//
//	Format: 100,VersionHeader,DateTime,FromParticipant,ToParticipant
func parseHeaderRecord(line string) (HeaderRecord, error) {
	f := strings.Split(line, ",")
	if len(f) < 5 {
		return HeaderRecord{}, fmt.Errorf("expected at least 5 fields, got %d", len(f))
	}
	dt, err := time.Parse("200601021504", f[2])
	if err != nil {
		return HeaderRecord{}, fmt.Errorf("parsing DateTime %q: %w", f[2], err)
	}
	return HeaderRecord{
		VersionHeader:   f[1],
		DateTime:        dt,
		FromParticipant: f[3],
		ToParticipant:   f[4],
	}, nil
}

// parseNMIDataDetailsRecord parses a NEM12 200 record.
//
//	Format: 200,NMI,NMIConfiguration,RegisterID,NMISuffix,MDMDataStreamID,
//	             MeterSerialNumber,UOM,IntervalLength,NextScheduledReadDate
func parseNMIDataDetailsRecord(line string) (NMIDataDetailsRecord, error) {
	f := strings.Split(line, ",")
	if len(f) < 10 {
		return NMIDataDetailsRecord{}, fmt.Errorf("expected at least 10 fields, got %d", len(f))
	}

	intervalLength, err := strconv.Atoi(f[8])
	if err != nil {
		return NMIDataDetailsRecord{}, fmt.Errorf("parsing IntervalLength %q: %w", f[8], err)
	}

	var nextReadDate time.Time
	if f[9] != "" {
		nextReadDate, err = time.Parse("20060102", f[9])
		if err != nil {
			return NMIDataDetailsRecord{}, fmt.Errorf("parsing NextScheduledReadDate %q: %w", f[9], err)
		}
	}

	return NMIDataDetailsRecord{
		NMI:                   f[1],
		NMIConfiguration:      f[2],
		RegisterID:            f[3],
		NMISuffix:             f[4],
		MDMDataStreamID:       f[5],
		MeterSerialNumber:     f[6],
		UOM:                   f[7],
		IntervalLength:        intervalLength,
		NextScheduledReadDate: nextReadDate,
	}, nil
}

// parseIntervalDataRecord parses a NEM12 300 record.
//
// The number of interval values is determined by intervalLength:
//
//	numIntervals = 24 * 60 / intervalLength   (e.g. 48 for 30-minute intervals)
//
// Layout:
//
//	300, IntervalDate, [numIntervals × value], QualityMethod,
//	ReasonCode, ReasonDescription, UpdateDateTime, MSATSLoadDateTime
func parseIntervalDataRecord(line string, intervalLength int) (IntervalDataRecord, error) {
	f := strings.Split(line, ",")

	numIntervals := (24 * 60) / intervalLength

	// Minimum: record type + date + all interval values + quality method
	minFields := 1 + 1 + numIntervals + 1
	if len(f) < minFields {
		return IntervalDataRecord{}, fmt.Errorf(
			"expected at least %d fields for intervalLength=%d, got %d",
			minFields, intervalLength, len(f),
		)
	}

	intervalDate, err := time.Parse("20060102", f[1])
	if err != nil {
		return IntervalDataRecord{}, fmt.Errorf("parsing IntervalDate %q: %w", f[1], err)
	}

	values := make([]float64, numIntervals)
	for i := 0; i < numIntervals; i++ {
		v, err := strconv.ParseFloat(f[2+i], 64)
		if err != nil {
			return IntervalDataRecord{}, fmt.Errorf(
				"parsing interval value %d %q: %w", i+1, f[2+i], err,
			)
		}
		values[i] = v
	}

	// Trailing metadata fields start right after the last interval value.
	qi := 2 + numIntervals // index of QualityMethod field

	rec := IntervalDataRecord{
		IntervalDate:   intervalDate,
		IntervalValues: values,
	}

	if qi < len(f) {
		rec.QualityMethod = f[qi]
	}
	if qi+1 < len(f) {
		rec.ReasonCode = f[qi+1]
	}
	if qi+2 < len(f) {
		rec.ReasonDescription = f[qi+2]
	}
	if qi+3 < len(f) && f[qi+3] != "" {
		rec.UpdateDateTime, err = time.Parse("20060102150405", f[qi+3])
		if err != nil {
			return IntervalDataRecord{}, fmt.Errorf("parsing UpdateDateTime %q: %w", f[qi+3], err)
		}
	}
	if qi+4 < len(f) && f[qi+4] != "" {
		rec.MSATSLoadDateTime, err = time.Parse("20060102150405", f[qi+4])
		if err != nil {
			return IntervalDataRecord{}, fmt.Errorf("parsing MSATSLoadDateTime %q: %w", f[qi+4], err)
		}
	}

	return rec, nil
}

// parseIntervalEventRecord parses a NEM12 400 record.
//
//	Format: 400,StartInterval,EndInterval,QualityMethod[,ReasonCode[,ReasonDescription]]
func parseIntervalEventRecord(line string) (IntervalEventRecord, error) {
	f := strings.Split(line, ",")
	if len(f) < 4 {
		return IntervalEventRecord{}, fmt.Errorf("expected at least 4 fields, got %d", len(f))
	}

	startInterval, err := strconv.Atoi(f[1])
	if err != nil {
		return IntervalEventRecord{}, fmt.Errorf("parsing StartInterval %q: %w", f[1], err)
	}
	endInterval, err := strconv.Atoi(f[2])
	if err != nil {
		return IntervalEventRecord{}, fmt.Errorf("parsing EndInterval %q: %w", f[2], err)
	}

	rec := IntervalEventRecord{
		StartInterval: startInterval,
		EndInterval:   endInterval,
		QualityMethod: f[3],
	}
	if len(f) > 4 {
		rec.ReasonCode = f[4]
	}
	if len(f) > 5 {
		rec.ReasonDescription = f[5]
	}
	return rec, nil
}

// parseB2BDetailsRecord parses a NEM12 500 record.
//
//	Format: 500,TransCode,RetServiceOrder,ReadDateTime[,IsMeteredCustomer]
func parseB2BDetailsRecord(line string) (B2BDetailsRecord, error) {
	f := strings.Split(line, ",")
	if len(f) < 4 {
		return B2BDetailsRecord{}, fmt.Errorf("expected at least 4 fields, got %d", len(f))
	}

	var readDateTime time.Time
	var err error
	if f[3] != "" {
		readDateTime, err = time.Parse("20060102150405", f[3])
		if err != nil {
			return B2BDetailsRecord{}, fmt.Errorf("parsing ReadDateTime %q: %w", f[3], err)
		}
	}

	isMetered := len(f) > 4 && f[4] == "Y"

	return B2BDetailsRecord{
		TransCode:         f[1],
		RetServiceOrder:   f[2],
		ReadDateTime:      readDateTime,
		IsMeteredCustomer: isMetered,
	}, nil
}

// --------------------------------------------------------------------------
// CLI entry point
// --------------------------------------------------------------------------

// dbFile is the path to the persistent SQLite database written on every run.
const dbFile = "meter_readings.db"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: nem12 <nem12-file>")
		os.Exit(2)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	db, err := OpenDB(dbFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := ResetSchema(db); err != nil {
		fmt.Fprintf(os.Stderr, "schema reset error: %v\n", err)
		os.Exit(1)
	}

	// readingsCh bridges the parser and the DB writer pool. It is buffered so
	// that the parser can run ahead of the writers without blocking.
	readingsCh := make(chan MeterReading, dbWorkerPoolSize*256)

	// Start the DB writer in the background. writeErrCh receives exactly one
	// value — the first write error or nil — once readingsCh is closed and all
	// workers have drained.
	writeErrCh := make(chan error, 1)
	go func() {
		writeErrCh <- StreamWriteReadings(db, readingsCh)
	}()

	// Stream the file one logical record at a time. Each IntervalDataRecord is
	// immediately expanded into per-interval MeterReadings and sent to the DB
	// writer, so neither the full record list nor the full reading list ever
	// accumulates in memory.
	parser := NewParser(f)

	var (
		currentNMI     string
		intervalLength = 30 // overwritten by each 200 record
		readingCount   int
	)

	fmt.Printf("streaming to %s using %d workers…\n", dbFile, dbWorkerPoolSize)

	for {
		rec, err := parser.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
			os.Exit(1)
		}

		// Print the record to stdout for manual inspection.
		switch v := rec.(type) {
		case HeaderRecord:
			fmt.Printf("[100] NEM12 | Created: %s | From: %s → To: %s\n",
				v.DateTime.Format("2006-01-02 15:04"), v.FromParticipant, v.ToParticipant)

		case NMIDataDetailsRecord:
			fmt.Printf("[200] NMI: %-12s | UOM: %-4s | Interval: %d min\n",
				v.NMI, v.UOM, v.IntervalLength)
			currentNMI = v.NMI
			intervalLength = v.IntervalLength

		case IntervalDataRecord:
			total := 0.0
			for _, val := range v.IntervalValues {
				total += val
			}
			fmt.Printf("[300] Date: %s | Periods: %2d | Daily total: %.3f\n",
				v.IntervalDate.Format("2006-01-02"), len(v.IntervalValues), total)

			// Expand each interval period into a MeterReading and hand it off
			// to the DB writer immediately — no slice accumulation.
			for i, val := range v.IntervalValues {
				ts := v.IntervalDate.Add(time.Duration(i*intervalLength) * time.Minute)
				readingsCh <- MeterReading{
					NMI:         currentNMI,
					Timestamp:   ts,
					Consumption: val,
				}
				readingCount++
			}

		case IntervalEventRecord:
			fmt.Printf("[400] Intervals %d–%d | Quality: %s | Reason: %s\n",
				v.StartInterval, v.EndInterval, v.QualityMethod, v.ReasonCode)

		case B2BDetailsRecord:
			fmt.Printf("[500] TransCode: %s | ServiceOrder: %s | Read: %s\n",
				v.TransCode, v.RetServiceOrder, v.ReadDateTime.Format("2006-01-02 15:04:05"))

		case EndOfDataRecord:
			fmt.Println("[900] End of data")
		}
	}

	// Signal to the writer pool that no more readings are coming, then wait
	// for it to finish before reporting the final count.
	close(readingsCh)

	if err := <-writeErrCh; err != nil {
		fmt.Fprintf(os.Stderr, "write error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("done – %d rows persisted\n", readingCount)
}
