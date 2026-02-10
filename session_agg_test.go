// test/unit/org/apache/cassandra/cql3/validation/operations/AggregationTest.java
package gocqlmem

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
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

	// Star with other aggregates
	assertIterSliceMap(t, "map[count(*):0 max(a):<nil>]", "", s, "SELECT count(*), max(a) FROM ks1.t1")

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

func TestMinAggregationDescending(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1)", existingRowMap)

	assertIterScan(t, "[[3 1]]", s, "SELECT count(b), min(b) as min FROM ks1.t1 WHERE a = 1")

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (2, 4000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (3, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (4, 0)", existingRowMap)

	assertIterScan(t, "[[6 0]]", s, "SELECT count(b), min(b) as min FROM ks1.t1")
}

func TestMaxAggregationAscending(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)").Exec())

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

func TestMinAggregationAscending(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, primary key (a, b)) WITH CLUSTERING ORDER BY (b ASC)").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (1, 1)", existingRowMap)

	assertIterScan(t, "[[3 1]]", s, "SELECT count(b), min(b) as min FROM ks1.t1 WHERE a = 1")

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (2, 4000)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (3, 100)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b) VALUES (4, 0)", existingRowMap)

	assertIterScan(t, "[[6 0]]", s, "SELECT count(b), min(b) as min FROM ks1.t1")
}

func TestAggregateWithColumns(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, c int, primary key (a, b))").Exec())

	assertIterSliceMap(t, "map[b:<nil> count(b):0 first:<nil> max:<nil>]", "", s, "SELECT count(b), max(b) as max, b, c as first FROM ks1.t1")
	assertIterScan(t, "[[0 <nil> <nil> <nil>]]", s, "SELECT count(b), max(b) as max, b, c as first FROM ks1.t1")

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 2, null)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (2, 4, 6)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (4, 8, 12)", existingRowMap)

	assertIterScan(t, "[[3 8 2 <nil>]]", s, "SELECT count(b), max(b) as max, b, c as first FROM ks1.t1")
}

func TestAggregateOnCounters(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b counter, primary key (a))").Exec())

	assertIterSliceMap(t, "map[b:<nil> count(b):0 max:<nil>]", "", s, "SELECT count(b), max(b) as max, b FROM ks1.t1")
	assertIterScan(t, "[[0 <nil> <nil> <nil>]]", s, "SELECT count(b), max(b) as max, b, c as first FROM ks1.t1")

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 1 WHERE a = 1", existingRowMap)
	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 1 WHERE a = 1", existingRowMap)
	assertIterScan(t, "[[1 2 2 2 2]]", s, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM ks1.t1")

	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 2 WHERE a = 1", existingRowMap)
	assertIterScan(t, "[[1 4 4 4 4]]", s, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM ks1.t1")

	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b - 2 WHERE a = 1", existingRowMap)
	assertIterScan(t, "[[1 2 2 2 2]]", s, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM ks1.t1")

	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 1 WHERE a = 2", existingRowMap)
	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 1 WHERE a = 2", existingRowMap)
	assertUpserMapScanCas(t, true, s, "UPDATE ks1.t1 SET b = b + 2 WHERE a = 2", existingRowMap)

	assertIterScan(t, "[[2 4 2 3 6]]", s, "SELECT count(b), max(b) as max, min(b) as min, avg(b) as avg, sum(b) as sum FROM ks1.t1")
}

func TestAggregateWithSetsListMapsTuplesUdtsFunctionsTtlSchemachange(t *testing.T) {
	// Not supported
}

func TestInvalidCalls(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, c int, primary key (a, b))").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 1, 10)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 2, 9)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 3, 8)", existingRowMap)

	iter := s.Query("SELECT max(b), max(c) FROM ks1.t1 WHERE max(a) = 1").Iter()
	assert.Contains(t, iter.err.Error(), "cannot evaluate where expression: cannot evaluate max(), context aggregate not enabled")

	iter = s.Query("SELECT max(b), max(c) FROM ks1.t1 WHERE max(a)").Iter()
	assert.Contains(t, iter.err.Error(), "cannot evaluate where expression: cannot evaluate max(), context aggregate not enabled")
}

func TestReversedType(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (a int, b int, c int, primary key (a, b)) WITH CLUSTERING ORDER BY (b DESC)").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 1, 10)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 2, 9)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 3, 8)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (a, b, c) VALUES (1, 4, 7)", existingRowMap)
	assertIterScan(t, "[[9 7 8]]", s, "SELECT max(c), min(c), avg(c) FROM ks1.t1 WHERE a = 1 AND b > 1")
}

func TestArithmeticCorrectness(t *testing.T) {
	s := NewSession()
	assert.Nil(t, s.Query("CREATE KEYSPACE ks1").Exec())
	assert.Nil(t, s.Query("CREATE TABLE ks1.t1 (bucket int primary key, val decimal)").Exec())

	existingRowMap := map[string]interface{}{}

	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (bucket, val) values (1, 0.25)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (bucket, val) values (2, 0.25)", existingRowMap)
	assertUpserMapScanCas(t, true, s, "INSERT INTO ks1.t1 (bucket, val) values (3, 0.5)", existingRowMap)
	result := decimal.NewFromFloat(0.25)
	result = result.Add(decimal.NewFromFloat(0.25))
	result = result.Add(decimal.NewFromFloat(0.5))
	result = result.Div(decimal.NewFromFloat(3.0))
	assertIterScan(t, fmt.Sprintf("[[%s]]", result.String()), s, "SELECT avg(val) FROM ks1.t1 where bucket in (1, 2, 3)")
}

/*
    @Test
    public void testAggregatesWithoutOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    (byte) ((Byte.MAX_VALUE / 3) + i), (short) ((Short.MAX_VALUE / 3) + i), (Integer.MAX_VALUE / 3) + i, (Long.MAX_VALUE / 3) + i,
                    BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(i)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row((byte) ((Byte.MAX_VALUE / 3) + 2), (short) ((Short.MAX_VALUE / 3) + 2), (Integer.MAX_VALUE / 3) + 2, (Long.MAX_VALUE / 3) + 2,
                       BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(2))));

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i + 3,
                    (byte) (100 + i), (short) (100 + i), 100 + i, 100L + i, BigInteger.valueOf(100 + i));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (4, 5, 6);"),
                   row((byte) 102, (short) 102, 102, 102L, BigInteger.valueOf(102)));
    }

    @Test
    public void testAggregateOverflow() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 tinyint, v2 smallint, v3 int, v4 bigint, v5 varint)");
        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2))));

        execute("truncate %s");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3, v4, v5) values (?, ?, ?, ?, ?, ?)", i,
                    Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3), avg(v4), avg(v5) from %s where bucket in (1, 2, 3);"),
                   row(Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE, BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(2))));

    }

    @Test
    public void testDoubleAggregatesPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 3; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i,
                    Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2)));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (1, 2, 3);"),
                   row(Float.MAX_VALUE, Double.MAX_VALUE, BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(2))));

        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 4, (float) 100.10, 100.10, BigDecimal.valueOf(100.10));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 5, (float) 110.11, 110.11, BigDecimal.valueOf(110.11));
        execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", 6, (float) 120.12, 120.12, BigDecimal.valueOf(120.12));

        assertRows(execute("select avg(v1), avg(v2), avg(v3) from %s where bucket in (4, 5, 6);"),
                   row((float) 110.11, 110.11, BigDecimal.valueOf(110.11)));
    }

    @Test
    public void testNan() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");

        for (int i = 1; i <= 10; i++)
            if (i != 5)
                execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

        execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, Float.NaN, Double.NaN);

        assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
        assertRows(execute("select sum(v1), sum(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                   row(Float.NaN, Double.NaN));
    }

    @Test
    public void testInfinity() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double)");
        for (boolean positive: new boolean[] { true, false})
        {
            final float FLOAT_INFINITY = positive ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
            final double DOUBLE_INFINITY = positive ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;

            for (int i = 1; i <= 10; i++)
                if (i != 5)
                    execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", i, (float) i, (double) i);

            execute("insert into %s (bucket, v1, v2) values (?, ?, ?)", 5, FLOAT_INFINITY, DOUBLE_INFINITY);

            assertRows(execute("select avg(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));
            assertRows(execute("select sum(v1), avg(v2) from %s where bucket in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10);"),
                       row(FLOAT_INFINITY, DOUBLE_INFINITY));

            execute("truncate %s");
        }
    }

    @Test
    public void testSumPrecision() throws Throwable
    {
        createTable("create table %s (bucket int primary key, v1 float, v2 double, v3 decimal)");

        for (int i = 1; i <= 17; i++)
            execute("insert into %s (bucket, v1, v2, v3) values (?, ?, ?, ?)", i, (float) (i / 10.0), i / 10.0, BigDecimal.valueOf(i / 10.0));

        assertRows(execute("select sum(v1), sum(v2), sum(v3) from %s;"),
                   row((float) 15.3, 15.3, BigDecimal.valueOf(15.3)));
    }

    @Test
    public void testRejectInvalidAggregateNamesOnCreation()
    {
        for (String funcName : Arrays.asList("my/fancy/aggregate", "my_other[fancy]aggregate"))
        {
            assertThatThrownBy(() -> {
                createAggregateOverload(String.format("%s.\"%s\"", KEYSPACE_PER_TEST, funcName), "int",
                                        " CREATE AGGREGATE IF NOT EXISTS %s(text, text)\n" +
                                        " SFUNC func\n" +
                                        " STYPE map<text,bigint>\n" +
                                        " INITCOND { };");
            }).hasRootCauseInstanceOf(InvalidRequestException.class)
              .hasRootCauseMessage("Aggregate name '%s' is invalid", funcName);
        }
    }
}
*/
