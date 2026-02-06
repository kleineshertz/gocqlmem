// test/unit/org/apache/cassandra/cql3/validation/operations/AggregationTest.java
package gocqlmem

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctions(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, c double, d decimal, e smallint, f tinyint, primary key (a, b))").Exec())

	// Test with empty table

	assertIterSliceMap(t,
		"map[avg(b):0 avg(c):0 avg(d):0 avg(e):0 avg(f):0 max(b):<nil> max(c):<nil> max(e):<nil> max(f):<nil> min(b):<nil> min(e):<nil> min(f):<nil> sum(b):0 sum(c):0 sum(d):0 sum(e):0 sum(f):0]", "", s,
		"SELECT max(b), min(b), sum(b), avg(b), max(c), sum(c), avg(c), sum(d), avg(d), max(e), min(e), sum(e), avg(e), max(f), min(f), sum(f), avg(f) FROM ks1.t1")
	assertIterScan(t,
		`[[<nil> <nil> 0 0 <nil> 0 0 0 0 <nil> <nil> 0 0 <nil> <nil> 0 0]]`, s,
		"SELECT max(b), min(b), sum(b), avg(b), max(c), sum(c), avg(c), sum(d), avg(d), max(e), min(e), sum(e), avg(e), max(f), min(f), sum(f), avg(f) FROM ks1.t1")

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c, d, e, f) VALUES (1, 1, 11.5, 11.5, 1, 1)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c, d, e, f) VALUES (1, 2, 9.5, 1.5, 2, 2)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c, d, e, f) VALUES (1, 3, 9.0, 2.0, 3, 3)", existingRowMap)

	assertIterScan(t,
		`[[3 1 6 2 11.5 30 10 15 5 3 1 6 2 3 1 6 2]]`, s,
		"SELECT max(b), min(b), sum(b), avg(b) , max(c), sum(c), avg(c), sum(d), avg(d),max(e), min(e), sum(e), avg(e),max(f), min(f), sum(f), avg(f) FROM ks1.t1")

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, d) VALUES (1, 5, 1.0)", existingRowMap)

	assertIterScan(t, `[[4]]`, s, "SELECT COUNT(*) FROM ks1.t1")
	assertIterScan(t, `[[4]]`, s, "SELECT COUNT(1) FROM ks1.t1")
	assertIterScan(t, `[[4 3 3 3]]`, s, "SELECT COUNT(b), count(c), count(e), count(f) FROM ks1.t1")

	// Makes sure that LIMIT does not affect the result of aggregates

	assertIterScan(t, `[[4 3 3 3]]`, s, "SELECT COUNT(b), count(c), count(e), count(f) FROM ks1.t1 LIMIT 2")
	assertIterScan(t, `[[4 3 3 3]]`, s, "SELECT COUNT(b), count(c), count(e), count(f) FROM ks1.t1 WHERE a = 1 LIMIT 2")
	assertIterScan(t, `[[2.75]]`, s, "SELECT AVG(CAST(b AS double)) FROM ks1.t1")
}

func TestCountStar(t *testing.T) {
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

func TestMaxAggregationDescending(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1)", existingRowMap)

	assertIterScan(t, "[[3 1000]]", s, "SELECT count(b), max(b) as max FROM ks1.t1 WHERE a = 1")

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (2, 4000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (3, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (4, 0)", existingRowMap)

	assertIterScan(t, "[[6 4000]]", s, "SELECT count(b), max(b) as max FROM ks1.t1")
}
