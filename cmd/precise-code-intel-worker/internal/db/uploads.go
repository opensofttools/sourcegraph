package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

func (db *dbImpl) Dequeue(ctx context.Context) (_ Upload, _ TxCloser, _ bool, err error) {
	selectionQuery := `
		UPDATE lsif_uploads u SET state = 'processing', started_at = now() WHERE id = (
			SELECT id FROM lsif_uploads
			WHERE state = 'queued'
			ORDER BY uploaded_at
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING u.id
	`

	id, err := scanInt(db.queryRow(ctx, sqlf.Sprintf(selectionQuery)))
	if err != nil {
		return Upload{}, nil, false, ignoreErrNoRows(err)
	}

	tw, err := db.beginTx(ctx)
	if err != nil {
		return Upload{}, nil, false, err
	}
	defer func() {
		if err != nil {
			err = closeTx(tw.tx, err)
		}
	}()

	fetchQuery := `SELECT * FROM lsif_uploads WHERE id = %s FOR UPDATE LIMIT 1`
	upload, err := scanUpload(tw.queryRow(ctx, sqlf.Sprintf(fetchQuery, id)))
	if err != nil {
		// TODO - can retry if no rows
		return Upload{}, nil, false, ignoreErrNoRows(err)
	}

	return upload, &txCloser{tw.tx}, true, nil
}

//
// TODO - call these from the txcloser above?
//

func (db *dbImpl) markComplete(ctx context.Context, tw *transactionWrapper, id int) error {
	query := `
		UPDATE lsif_uploads
		SET state = 'completed', finished_at = now()
		WHERE id = %s
	`

	_, err := tw.exec(ctx, sqlf.Sprintf(query, id))
	return err
}

func (db *dbImpl) markErrored(ctx context.Context, tw *transactionWrapper, id int, failureSummary, failureStacktrace string) error {
	query := `
		UPDATE lsif_uploads
		SET state = 'errored', finished_at = now(), failure_summary = %s, failure_stacktrace = %s
		WHERE id = %s
	`

	_, err := tw.exec(ctx, sqlf.Sprintf(query, failureSummary, failureStacktrace, id))
	return err
}
