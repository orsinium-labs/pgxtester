//nolint:ireturn
package main

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

type Config struct {
	// Connection URL to be used if Conn is not provided.
	URL string
	// The connection to wrap around. If nil, a new connection will be established using URL.
	Conn *pgx.Conn
	// The timeout for establishing connection, creating trnsaction, and rolling it back.
	//
	// Default is 2 seconds.
	Timeout time.Duration
	// The options for creating a transaction.
	TxOptions pgx.TxOptions
}

func Connect(t *testing.T, c Config) DBTX {
	if c.Timeout == 0 {
		c.Timeout = 2 * time.Second
	}
	if c.Conn == nil {
		if c.URL == "" {
			c.URL = os.Getenv("POSTGRES_URL")
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
		defer cancel()
		conn, err := pgx.Connect(ctx, c.URL)
		if err != nil {
			t.Fatalf("failed to connect to PostgreSQL: %v", err)
		}
		c.Conn = conn
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	tx, err := c.Conn.BeginTx(ctx, c.TxOptions)
	if err != nil {
		t.Fatalf("failed to begin tansaction: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
		defer cancel()
		err := tx.Rollback(ctx)
		if err != nil {
			t.Fatalf("failed to rollback transaction: %v", err)
		}
	})

	wrapper := blockingDB{tx: tx}
	return &wrapper
}

// A wrapper around DB connection that is safe to be used concurrently.
//
// It is similar to pgxpool except it keeps only a single connection,
// and so it can be safely rolled back. It is slower than pgxpool
// so it must be used only in tests.
type blockingDB struct {
	tx DBTX
	mx sync.Mutex
}

func (db *blockingDB) Exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	db.mx.Lock()
	defer db.mx.Unlock()
	return db.tx.Exec(ctx, q, args...)
}

func (db *blockingDB) Query(ctx context.Context, q string, args ...any) (pgx.Rows, error) {
	db.mx.Lock()
	defer db.mx.Unlock()
	return db.tx.Query(ctx, q, args...) //nolint:sqlclosecheck
}

func (db *blockingDB) QueryRow(ctx context.Context, q string, args ...any) pgx.Row {
	db.mx.Lock()
	defer db.mx.Unlock()
	return db.tx.QueryRow(ctx, q, args...)
}

func (db *blockingDB) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
) (int64, error) {
	db.mx.Lock()
	defer db.mx.Unlock()
	return db.tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}
