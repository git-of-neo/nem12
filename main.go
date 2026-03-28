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
// It is used by stitchLines to detect the start of a new logical record.
var knownRecordTypes = map[string]bool{
	"100": true,
	"200": true,
	"300": true,
	"400": true,
	"500": true,
	"900": true,
}

// Parse reads a NEM12 file from r and returns the parsed records in file order.
//
// NEM12 files sometimes contain physical line breaks in the middle of a logical
// record — for example when a number such as "1.271" is wrapped across two lines
// as "1." and "271". Parse handles this transparently by stitching physical lines
// back into complete logical records before parsing any fields.
func Parse(r io.Reader) ([]Record, error) {
	logicalLines, err := stitchLines(r)
	if err != nil {
		return nil, fmt.Errorf("reading NEM12 input: %w", err)
	}

	var records []Record

	// intervalLength is carried forward from the most recent 200 record so that
	// subsequent 300 records know how many interval values to expect.
	currentIntervalLength := 30

	for _, line := range logicalLines {
		if line == "" {
			continue
		}

		// The record type is always the first comma-separated token.
		typeStr, _, _ := strings.Cut(line, ",")

		switch typeStr {
		case "100":
			rec, err := parseHeaderRecord(line)
			if err != nil {
				return nil, fmt.Errorf("100 record: %w", err)
			}
			records = append(records, rec)

		case "200":
			rec, err := parseNMIDataDetailsRecord(line)
			if err != nil {
				return nil, fmt.Errorf("200 record: %w", err)
			}
			currentIntervalLength = rec.IntervalLength
			records = append(records, rec)

		case "300":
			rec, err := parseIntervalDataRecord(line, currentIntervalLength)
			if err != nil {
				return nil, fmt.Errorf("300 record: %w", err)
			}
			records = append(records, rec)

		case "400":
			rec, err := parseIntervalEventRecord(line)
			if err != nil {
				return nil, fmt.Errorf("400 record: %w", err)
			}
			records = append(records, rec)

		case "500":
			rec, err := parseB2BDetailsRecord(line)
			if err != nil {
				return nil, fmt.Errorf("500 record: %w", err)
			}
			records = append(records, rec)

		case "900":
			records = append(records, EndOfDataRecord{})

		default:
			return nil, fmt.Errorf("unknown record type %q", typeStr)
		}
	}

	return records, nil
}

// stitchLines reads all physical lines from r and joins continuation lines
// (those that do not begin with a known NEM12 record-type identifier) directly
// onto the preceding line with no separator. This correctly reconstructs numbers
// and field values that were split at an arbitrary column width.
func stitchLines(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)

	var logical []string
	var current strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Check whether this physical line starts a new logical record by
		// inspecting its first comma-delimited token.
		prefix, _, _ := strings.Cut(line, ",")
		if knownRecordTypes[prefix] {
			// Save the completed logical line before starting the next one.
			if current.Len() > 0 {
				logical = append(logical, current.String())
				current.Reset()
			}
		}

		// Append without any separator: a continuation line resumes exactly
		// where the previous one left off, even mid-number or mid-field.
		current.WriteString(line)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if current.Len() > 0 {
		logical = append(logical, current.String())
	}

	return logical, nil
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

	records, err := Parse(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse error: %v\n", err)
		os.Exit(1)
	}

	for _, rec := range records {
		switch v := rec.(type) {
		case HeaderRecord:
			fmt.Printf("[100] NEM12 | Created: %s | From: %s → To: %s\n",
				v.DateTime.Format("2006-01-02 15:04"), v.FromParticipant, v.ToParticipant)

		case NMIDataDetailsRecord:
			fmt.Printf("[200] NMI: %-12s | UOM: %-4s | Interval: %d min\n",
				v.NMI, v.UOM, v.IntervalLength)

		case IntervalDataRecord:
			total := 0.0
			for _, val := range v.IntervalValues {
				total += val
			}
			fmt.Printf("[300] Date: %s | Periods: %2d | Daily total: %.3f\n",
				v.IntervalDate.Format("2006-01-02"), len(v.IntervalValues), total)

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

	readings := RecordsToReadings(records)
	fmt.Printf("writing %d readings to %s using %d workers…\n",
		len(readings), dbFile, dbWorkerPoolSize)

	if err := WriteReadings(db, readings); err != nil {
		fmt.Fprintf(os.Stderr, "write error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("done – %d rows persisted\n", len(readings))
}
