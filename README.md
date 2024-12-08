# pgxtester

A Go package for testing code that uses PostgreSQL. It establishes a concurrency-safe [pgx](https://github.com/jackc/pgx) connection, wraps it into a transaction, and rolls back the transaction at the end of the test. This ensures that changes done in a test will not affect other tests.

## Installation

```bash
go get github.com/orsinium-labs/pgxtester
```

## Usage

```go
func SomeTest(t *testing.T) {
    conn := pgxtester.Connect(t, pgxtester.Config{URL: "..."})
    // ...
}
```
