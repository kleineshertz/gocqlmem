#!/bin/bash

go test -coverprofile=/var/tmp/gocqlmem.p.tmp -cover $(find ./ -name '*_test.go' -printf "%h\n" | sort -u)
cat /var/tmp/gocqlmem.p.tmp | grep -v "donotcover" > /var/tmp/gocqlmem.p
go tool cover -html=/var/tmp/gocqlmem.p -o=/var/tmp/gocqlmem.html
echo See /var/tmp/gocqlmem.html