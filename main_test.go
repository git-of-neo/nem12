package main

import (
	_ "embed"
	"strings"
	"testing"
	"time"
)

// sampleNEM12 is the provided sample NEM12 file embedded at compile time.
//
//go:embed data/sample.NEM12
var sampleNEM12 string

// mustParse is a test helper that parses the embedded sample and fails the
// test immediately if any error occurs.
func mustParse(t *testing.T) []Record {
	t.Helper()
	records, err := Parse(strings.NewReader(sampleNEM12))
	if err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	return records
}

// TestParse_RecordCount verifies that the parser produces the expected total
// number of records and the right mix of types from the sample file.
func TestParse_RecordCount(t *testing.T) {
	records := mustParse(t)

	counts := map[RecordType]int{}
	for _, r := range records {
		counts[r.Type()]++
	}

	want := map[RecordType]int{
		RecordTypeHeader:         1,
		RecordTypeNMIDataDetails: 2,
		RecordTypeIntervalData:   8, // 4 days × 2 NMI blocks
		RecordTypeB2BDetails:     2,
		RecordTypeEndOfData:      1,
	}

	for typ, wantCount := range want {
		if got := counts[typ]; got != wantCount {
			t.Errorf("record type %d: got %d records, want %d", typ, got, wantCount)
		}
	}

	const wantTotal = 14
	if len(records) != wantTotal {
		t.Errorf("total records: got %d, want %d", len(records), wantTotal)
	}
}

// TestParse_NMI verifies that the NMI field of the first NMIDataDetails (200) record
// matches the expected value from the sample file ("NEM1201009").
func TestParse_NMI(t *testing.T) {
	records := mustParse(t)

	const wantNMI = "NEM1201009"

	for _, rec := range records {
		nmi, ok := rec.(NMIDataDetailsRecord)
		if !ok {
			continue
		}
		if nmi.NMI != wantNMI {
			t.Errorf("first NMIDataDetailsRecord.NMI = %q, want %q", nmi.NMI, wantNMI)
		}
		return // only check the first NMIDataDetails (200) record
	}

	t.Fatal("no NMIDataDetailsRecord found in parsed records")
}

// TestParse_IntervalLength verifies that the interval length from the first
// 200 record is 30 minutes, as specified in the sample file.
func TestParse_IntervalLength(t *testing.T) {
	records := mustParse(t)

	const wantLength = 30

	for _, rec := range records {
		nmi, ok := rec.(NMIDataDetailsRecord)
		if !ok {
			continue
		}
		if nmi.IntervalLength != wantLength {
			t.Errorf("NMIDataDetailsRecord.IntervalLength = %d, want %d", nmi.IntervalLength, wantLength)
		}
		return
	}

	t.Fatal("no NMIDataDetailsRecord found in parsed records")
}

// TestParse_IntervalDate verifies that the first 300 record following the
// first 200 record has the interval date 2005-03-01.
func TestParse_IntervalDate(t *testing.T) {
	records := mustParse(t)

	wantDate := time.Date(2005, 3, 1, 0, 0, 0, 0, time.UTC)

	sawNMI := false
	for _, rec := range records {
		switch v := rec.(type) {
		case NMIDataDetailsRecord:
			sawNMI = true
		case IntervalDataRecord:
			if !sawNMI {
				continue
			}
			if !v.IntervalDate.Equal(wantDate) {
				t.Errorf("first IntervalDataRecord.IntervalDate = %s, want %s",
					v.IntervalDate.Format("2006-01-02"),
					wantDate.Format("2006-01-02"),
				)
			}
			return // only check the very first 300 record
		}
	}

	t.Fatal("no IntervalDataRecord found after NMIDataDetailsRecord")
}

// TestParse_ConsumptionValues verifies individual interval consumption values
// from the first 300 record of the first NMI block ("NEM1201009", 2005-03-01).
//
// For 30-minute intervals there are 48 periods per day. The sample data has
// 12 zero-valued periods at the start of the day, followed by non-zero values
// beginning at index 12. Specific values asserted here are drawn directly from
// the four expectations in the design document.
func TestParse_ConsumptionValues(t *testing.T) {
	records := mustParse(t)

	// Locate the first IntervalDataRecord in the file.
	var firstInterval *IntervalDataRecord
	for _, rec := range records {
		if iv, ok := rec.(IntervalDataRecord); ok {
			firstInterval = &iv
			break
		}
	}
	if firstInterval == nil {
		t.Fatal("no IntervalDataRecord found in parsed records")
	}

	const wantNumIntervals = 48 // 24 * 60 / 30
	if len(firstInterval.IntervalValues) != wantNumIntervals {
		t.Fatalf("IntervalValues length = %d, want %d",
			len(firstInterval.IntervalValues), wantNumIntervals)
	}

	// The first 12 periods (midnight – 06:00) should be zero.
	for i := 0; i < 12; i++ {
		if firstInterval.IntervalValues[i] != 0 {
			t.Errorf("IntervalValues[%d] = %v, want 0", i, firstInterval.IntervalValues[i])
		}
	}

	// Spot-check individual non-zero values starting at period index 12.
	wantValues := []struct {
		index int
		value float64
	}{
		{12, 0.461}, // first non-zero value (06:00–06:30)
		{13, 0.810},
		{14, 0.568},
		{15, 1.234},
		{16, 1.353},
		{46, 0.345}, // second-to-last period
		{47, 0.231}, // last period
	}

	for _, tc := range wantValues {
		got := firstInterval.IntervalValues[tc.index]
		if got != tc.value {
			t.Errorf("IntervalValues[%d] = %v, want %v", tc.index, got, tc.value)
		}
	}
}

// TestParse_SecondNMIBlock verifies that the second 200 record (NEM1201010)
// is correctly parsed, confirming the parser resets NMI context between blocks.
func TestParse_SecondNMIBlock(t *testing.T) {
	records := mustParse(t)

	var nmiRecords []NMIDataDetailsRecord
	for _, rec := range records {
		if nmi, ok := rec.(NMIDataDetailsRecord); ok {
			nmiRecords = append(nmiRecords, nmi)
		}
	}

	if len(nmiRecords) < 2 {
		t.Fatalf("expected at least 2 NMIDataDetailsRecords, got %d", len(nmiRecords))
	}

	second := nmiRecords[1]
	if second.NMI != "NEM1201010" {
		t.Errorf("second NMI = %q, want %q", second.NMI, "NEM1201010")
	}
	if second.IntervalLength != 30 {
		t.Errorf("second NMI IntervalLength = %d, want 30", second.IntervalLength)
	}
}

// TestParse_Header verifies the 100 record fields from the sample file.
func TestParse_Header(t *testing.T) {
	records := mustParse(t)

	if len(records) == 0 {
		t.Fatal("no records returned")
	}

	header, ok := records[0].(HeaderRecord)
	if !ok {
		t.Fatalf("records[0] type = %T, want HeaderRecord", records[0])
	}

	if header.VersionHeader != "NEM12" {
		t.Errorf("VersionHeader = %q, want %q", header.VersionHeader, "NEM12")
	}
	if header.FromParticipant != "UNITEDDP" {
		t.Errorf("FromParticipant = %q, want %q", header.FromParticipant, "UNITEDDP")
	}
	if header.ToParticipant != "NEMMCO" {
		t.Errorf("ToParticipant = %q, want %q", header.ToParticipant, "NEMMCO")
	}

	wantDT := time.Date(2005, 6, 8, 11, 49, 0, 0, time.UTC)
	if !header.DateTime.Equal(wantDT) {
		t.Errorf("DateTime = %v, want %v", header.DateTime, wantDT)
	}
}

// TestParse_EndOfData verifies that the last record in the file is a 900 record.
func TestParse_EndOfData(t *testing.T) {
	records := mustParse(t)

	if len(records) == 0 {
		t.Fatal("no records returned")
	}

	last := records[len(records)-1]
	if _, ok := last.(EndOfDataRecord); !ok {
		t.Errorf("last record type = %T, want EndOfDataRecord", last)
	}
}

// TestParse_B2BDetails verifies that the first 500 record is correctly parsed.
func TestParse_B2BDetails(t *testing.T) {
	records := mustParse(t)

	for _, rec := range records {
		b2b, ok := rec.(B2BDetailsRecord)
		if !ok {
			continue
		}
		if b2b.TransCode != "O" {
			t.Errorf("B2BDetailsRecord.TransCode = %q, want %q", b2b.TransCode, "O")
		}
		if b2b.RetServiceOrder != "S01009" {
			t.Errorf("B2BDetailsRecord.RetServiceOrder = %q, want %q", b2b.RetServiceOrder, "S01009")
		}
		wantRead := time.Date(2005, 3, 10, 12, 10, 4, 0, time.UTC)
		if !b2b.ReadDateTime.Equal(wantRead) {
			t.Errorf("B2BDetailsRecord.ReadDateTime = %v, want %v", b2b.ReadDateTime, wantRead)
		}
		return
	}

	t.Fatal("no B2BDetailsRecord found in parsed records")
}

// TestStitchLines_HandlesWrappedNumbers verifies that the line-stitching logic
// correctly reconstructs numbers that were split across physical line breaks,
// e.g. "1." on one line and "271" on the next becoming "1.271".
func TestStitchLines_HandlesWrappedNumbers(t *testing.T) {
	// Minimal 300 record with a number split across three physical lines.
	// "1.271" is intentionally broken as "1." + "271".
	input := strings.Join([]string{
		"100,NEM12,200506081149,SENDER,RECV",
		"200,NEM1200001,E1,1,E1,,METER1,kWh,30,20050610",
		"300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.",
		"271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.7",
		"31,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204",
		"900",
	}, "\n")

	records, err := Parse(strings.NewReader(input))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	var iv *IntervalDataRecord
	for _, r := range records {
		if v, ok := r.(IntervalDataRecord); ok {
			iv = &v
			break
		}
	}
	if iv == nil {
		t.Fatal("no IntervalDataRecord parsed")
	}

	// After stitching, index 21 should be 1.271 (not 1. or 271).
	const wantIdx = 21
	const wantVal = 1.271
	if got := iv.IntervalValues[wantIdx]; got != wantVal {
		t.Errorf("IntervalValues[%d] = %v, want %v (line-stitching may be broken)", wantIdx, got, wantVal)
	}

	// Index 36 should be 0.731 (reconstructed from "0.7" + "31").
	const wantIdx2 = 36
	const wantVal2 = 0.731
	if got := iv.IntervalValues[wantIdx2]; got != wantVal2 {
		t.Errorf("IntervalValues[%d] = %v, want %v (line-stitching may be broken)", wantIdx2, got, wantVal2)
	}
}
