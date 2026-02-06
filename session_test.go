package gocqlmem

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func sliceMapRowsToString(rows []map[string]any) string {
	if rows == nil {
		return ""
	}
	sb := strings.Builder{}
	for _, r := range rows {
		sb.WriteString(fmt.Sprintf("%v", r))
	}
	return sb.String()
}

func assertIterSliceMap(t *testing.T, expectedRows string, expectedErr string, s *Session, q string) {
	rows, err := s.Query(q).Iter().SliceMap()
	if expectedErr == "" {
		assert.Nil(t, err)
	} else {
		assert.Equal(t, expectedErr, err.Error())
	}
	assert.Equal(t, expectedRows, sliceMapRowsToString(rows))
}

func assertIterScan(t *testing.T, expectedRows string, s *Session, q string) {
	iter := s.Query(q).Iter()
	assert.Nil(t, iter.err)
	row := make([]interface{}, len(iter.RetrievedNames))
	sb := strings.Builder{}
	for iter.Scan(row...) {
		sb.WriteString(fmt.Sprintf("[%v]", row))
	}
	assert.Equal(t, expectedRows, sb.String())
}

func assertIterMapScan(t *testing.T, expectedRows string, s *Session, q string) {
	iter := s.Query(q).Iter()
	sb := strings.Builder{}
	row := map[string]interface{}{}
	for _, name := range iter.RetrievedNames {
		row[name] = nil
	}
	for iter.MapScan(row) {
		sb.WriteString(fmt.Sprintf("[%v]", row))
	}
	assert.Equal(t, expectedRows, sb.String())
}

func assertScanner(t *testing.T, expectedRows string, s *Session, q string) {
	iter := s.Query(q).Iter()
	row := make([]interface{}, len(iter.RetrievedNames))
	sb := strings.Builder{}
	scanner := iter.Scanner()
	for scanner.Next() {
		scanner.Scan(row...)
		sb.WriteString(fmt.Sprintf("[%v]", row))
	}
	assert.Equal(t, expectedRows, sb.String())
}

func assertUpserMapScanCas(t *testing.T, isApplyExpected bool, s *Session, q string, existingRowMap map[string]interface{}) {
	isApplied, err := s.Query(q).MapScanCAS(existingRowMap)
	assert.Nil(t, err)
	assert.Equal(t, isApplyExpected, isApplied)
}

func TestCountStar(t *testing.T) {
	// var rows []map[string]any

	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, c double, primary key (a, b))").Exec())

	// One star count
	assertIterSliceMap(t, "map[count(*):0]", "", s, "SELECT count(*) FROM ks1.t1")
	assertIterSliceMap(t, "map[count(t1.*):0]", "", s, "SELECT count(t1.*) FROM ks1.t1")

	// One star count with alias
	assertIterSliceMap(t, "map[cnt:0]", "", s, "SELECT count(*) AS cnt FROM ks1.t1")
	assertIterSliceMap(t, "map[cnt:0]", "", s, "SELECT count(t1.*) AS cnt FROM ks1.t1")

	// Two star counts
	assertIterSliceMap(t, "map[count(*):0 count(1):0]", "", s, "SELECT count(*), count(1) FROM ks1.t1")
	assertIterScan(t, "[[0 0]]", s, "SELECT count(*), count(*) FROM ks1.t1")
	assertIterMapScan(t, "[map[count(*):0]]", s, "SELECT count(*), count(*) FROM ks1.t1")
	assertIterMapScan(t, "[map[count(*):0 count(1):0]]", s, "SELECT count(*), count(1) FROM ks1.t1")
	assertScanner(t, "[[0 0]]", s, "SELECT count(*), count(*) FROM ks1.t1")

	// count(1)
	assertIterSliceMap(t, "map[count(1):0]", "", s, "SELECT count(1) FROM ks1.t1")
	assertIterSliceMap(t, "map[count(1):0]", "", s, "SELECT count(1) FROM ks1.t1")

	// count(1) with alias
	assertIterSliceMap(t, "map[cnt:0]", "", s, "SELECT count(1) AS cnt FROM ks1.t1")

	// Star with with other aggregates
	assertIterSliceMap(t, "map[avg(a):<nil> count(*):0]", "", s, "SELECT count(*), avg(a) FROM ks1.t1")

	existingRowMap := map[string]interface{}{}
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 1, 11.5)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 2, 9.5)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 3, 9.0)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 5, 1.0)", existingRowMap)

	assertIterScan(t, "[[4]]", s, "SELECT COUNT(*) FROM ks1.t1")
	assertIterScan(t, "[[4]]", s, "SELECT COUNT(1) FROM ks1.t1")
	assertIterScan(t, "[[5 1 4]]", s, "SELECT max(b), b, COUNT(*) FROM ks1.t1")
	assertIterScan(t, "[[5 4 1]]", s, "SELECT max(b), COUNT(1), b FROM ks1.t1")
	// // Makes sure that LIMIT does not affect the result of aggregates
	assertIterScan(t, "[[5 4 1]]", s, "SELECT max(b), COUNT(1), b FROM ks1.t1 LIMIT 2")
	assertIterScan(t, "[[5 4 1]]", s, "SELECT max(b), COUNT(1), b FROM ks1.t1 WHERE a = 1 LIMIT 2")
}
