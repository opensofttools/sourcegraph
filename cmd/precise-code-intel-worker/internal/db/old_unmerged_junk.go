package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/keegancsmith/sqlf"
	"github.com/sourcegraph/sourcegraph/internal/db/dbutil"
)

// This is stuff already defined in the API that's not merged yet

type dbImpl struct {
	db *sql.DB
}

var _ DB = &dbImpl{}

// New creates a new instance of DB connected to the given Postgres DSN.
func New(postgresDSN string) (DB, error) {
	db, err := dbutil.NewDB(postgresDSN, "precise-code-intel-api-server")
	if err != nil {
		return nil, err
	}

	return &dbImpl{db: db}, nil
}

// query performs Query on the underlying connection.
func (db *dbImpl) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// queryRow performs QueryRow on the underlying connection.
func (db *dbImpl) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return db.db.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// beginTx performs BeginTx on the underlying connection and wraps the transaction.
func (db *dbImpl) beginTx(ctx context.Context) (*transactionWrapper, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &transactionWrapper{tx}, nil
}

// TxCloser is a convenience wrapper for closing SQL transactions.
type TxCloser interface {
	// CloseTx commits the transaction on a nil error value and performs a rollback
	// otherwise. If an error occurs during commit or rollback of the transaction,
	// the error is added to the resulting error value.
	CloseTx(err error) error
}

type txCloser struct {
	tx *sql.Tx
}

func (txc *txCloser) CloseTx(err error) error {
	return closeTx(txc.tx, err)
}

func closeTx(tx *sql.Tx, err error) error {
	if err != nil {
		if rollErr := tx.Rollback(); rollErr != nil {
			err = multierror.Append(err, rollErr)
		}
		return err
	}

	return tx.Commit()
}

// Upload is a subset of the lsif_uploads table and stores both processed and unprocessed
// records.
type Upload struct {
	ID                int        `json:"id"`
	Commit            string     `json:"commit"`
	Root              string     `json:"root"`
	VisibleAtTip      bool       `json:"visibleAtTip"`
	UploadedAt        time.Time  `json:"uploadedAt"`
	State             string     `json:"state"`
	FailureSummary    *string    `json:"failureSummary"`
	FailureStacktrace *string    `json:"failureStacktrace"`
	StartedAt         *time.Time `json:"startedAt"`
	FinishedAt        *time.Time `json:"finishedAt"`
	TracingContext    string     `json:"tracingContext"`
	RepositoryID      int        `json:"repositoryId"`
	Indexer           string     `json:"indexer"`
	Rank              *int       `json:"placeInQueue"`
}

// Scanner is the common interface shared by *sql.Row and *sql.Rows.
type Scanner interface {
	// Scan copies the values of the current row into the values pointed at by dest.
	Scan(dest ...interface{}) error
}

// scanUpload populates an Upload value from the given scanner.
func scanUpload(scanner Scanner) (upload Upload, err error) {
	err = scanner.Scan(
		&upload.ID,
		&upload.Commit,
		&upload.Root,
		&upload.VisibleAtTip,
		&upload.UploadedAt,
		&upload.State,
		&upload.FailureSummary,
		&upload.FailureStacktrace,
		&upload.StartedAt,
		&upload.FinishedAt,
		&upload.TracingContext,
		&upload.RepositoryID,
		&upload.Indexer,
		&upload.Rank,
	)
	return
}

// ignoreErrNoRows returns the given error if it's not sql.ErrNoRows.
func ignoreErrNoRows(err error) error {
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

// scanInt populates an integer value from the given scanner.
func scanInt(scanner Scanner) (value int, err error) {
	err = scanner.Scan(&value)
	return
}

type transactionWrapper struct {
	tx *sql.Tx
}

// query performs QueryContext on the underlying transaction.
func (tw *transactionWrapper) query(ctx context.Context, query *sqlf.Query) (*sql.Rows, error) {
	return tw.tx.QueryContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// queryRow performs QueryRow on the underlying transaction.
func (tw *transactionWrapper) queryRow(ctx context.Context, query *sqlf.Query) *sql.Row {
	return tw.tx.QueryRowContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}

// exec performs Exec on the underlying transaction.
func (tw *transactionWrapper) exec(ctx context.Context, query *sqlf.Query) (sql.Result, error) {
	return tw.tx.ExecContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
}
