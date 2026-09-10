# <img src="gocqlmem.svg" alt="logo" width="60"/> gocqlmem <div style="float:right;"> [![coveralls](https://coveralls.io/repos/github/capillariesio/gocqlmem/badge.svg?branch=main)](https://coveralls.io/github/capillariesio/gocqlmem?branch=main) [![Go Reference](https://pkg.go.dev/badge/github.com/capillariesio/gocqlmem.svg)](https://pkg.go.dev/github.com/capillariesio/gocqlmem)</div>

Rationale: I have a project that uses <a href="https://github.com/apache/cassandra-gocql-driver">gocql (Cassandra gocql driver)</a> and I want to create unit tests that exercise pieces that actually call gocql.
Couldn't find a library that simulates Cassandra access via gocql, so created my own.

gocqlmem implements in-memory Cassandra engine and provides access to it via gocql-like interface. This is a spin-off from the original gocql-using project <a href="https://github.com/capillariesio/capillaries">Capillaries</a>.

Unfortunately, gocql exports objects, not interfaces. So, I had to create a set of interfaces that mimic gocql Session, Query, Iter and declare them in gocqlmem/gocqlshims. If gocql developers ever decide to expose interfaces (see proposed interfaces in  [gocqlshims/gocql_shims.go](./gocqlshims/gocql_shims.go), gocqlmem/gocqlshims will go away.

In your code, instead of gocql objects Iter, Query and Session, use shim interfaces instead:
- gocqlshims.Session
- gocqlshims.Query
- gocqlshims.Iter

So the caller should not know either original gocql implementation is called, or gocqlmem implementation.

Below is the sample code creating a gocqlshims.Session depending on the configuration (test/prod). For a complete sample use, see code in https://github.com/capillariesio/capillaries/blob/main/pkg/db/cassandra.go and the test in https://github.com/capillariesio/capillaries/blob/main/pkg/api/api_test.go.


```
package main

import (
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/capillariesio/gocqlmem"
	"github.com/capillariesio/gocqlmem/gocqlshims"
)

func NewSession(cfg *SomeConfig) (gocqlshims.Session, error) {
	if cfg.IsTest {
        return gocqlmem.NewGocqlmemSession(), nil
	}

	dataCluster := gocql.NewCluster(cfg.Hosts...)
	dataCluster.Port = cfg.Port
    ...
    cassandraSession, err := dataCluster.CreateSession()
    gocqlshimsSession := gocqlshims.NewGocqlSession(cassandraSession)
    ...
    return gocqlshimsSession, nil
}

// Use gocqlshims.Session, gocqlshims.Query, gocqlshims.Iter
```

Expression evaluation engine (see ./eval directory) is Go AST-based, it was borrowed from Capillaries, it is well-tested and seems to do the job. CQL parser was written from scratch as I could not make good use of generic parser packages like antlr4 or lex/yacc. gocql-specific code in gocqlmem often mimics gocql implementation, but it does not really have to.

At the moment, gocqlmem provides basic functionality. Some things like batches, complex Cassandra data types. A lot of TODOs across the code. Test code coverage is not stellar, run [test_coverage.sh](./test_coverage.sh) to try it.

(C) 2026 KH (kleines.hertz[at]protonmail.com)