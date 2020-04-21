package sqliteutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/keegancsmith/sqlf"
)

type BatchInserter struct {
	db                    *sqlx.DB
	tableName             string
	columnNames           []string
	batches               [][]interface{}
	maxBatchesBeforeFlush int
}

func NewBatchInserter(db *sqlx.DB, tableName string, columnNames ...string) *BatchInserter {
	return &BatchInserter{
		db:                    db,
		tableName:             tableName,
		columnNames:           columnNames,
		maxBatchesBeforeFlush: 999 / len(columnNames),
	}
}

func (i *BatchInserter) Insert(values ...interface{}) error {
	if len(values) != len(i.columnNames) {
		return fmt.Errorf("mismatched batch")
	}

	i.batches = append(i.batches, values)

	if len(i.batches) < i.maxBatchesBeforeFlush {
		return nil
	}

	return i.Flush()
}

func (i *BatchInserter) Flush() error {
	if len(i.batches) < i.maxBatchesBeforeFlush {
		return i.flushPartial()
	}
	return i.flushFull()
}

func (i *BatchInserter) flushFull() error {
	err := i.write(i.batches)
	i.batches = nil
	return err

}

func (i *BatchInserter) flushPartial() error {
	err := i.write(i.batches[:i.maxBatchesBeforeFlush])
	i.batches = i.batches[i.maxBatchesBeforeFlush:]
	return err
}

func (i *BatchInserter) write(batch [][]interface{}) error {
	query := "INSERT INTO " + i.tableName + " (" + strings.Join(i.columnNames, ", ") + ") VALUES (%s)"

	var queries []*sqlf.Query
	for _, args := range batch {
		var ps []string
		for range args {
			ps = append(ps, "%s")
		}

		queries = append(queries, sqlf.Sprintf("("+strings.Join(ps, ",")+")", args...))
	}

	qx := sqlf.Sprintf(query, sqlf.Join(queries, ","))
	_, err := i.db.ExecContext(context.Background(), qx.Query(sqlf.SimpleBindVar), qx.Args()...)
	return err
}
