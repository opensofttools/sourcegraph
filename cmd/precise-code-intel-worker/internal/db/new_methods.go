package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

type Package struct {
	Scheme  string
	Name    string
	Version string
}

type Reference struct {
	Scheme      string
	Name        string
	Version     string
	Identifiers []string // TODO - should be filter by now
}

// TODO - add?
func (db *dbImpl) exec(ctx context.Context, query *sqlf.Query) error {
	_, err := db.db.ExecContext(ctx, query.Query(sqlf.PostgresBindVar), query.Args()...)
	return err
}

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

func (db *dbImpl) UpdatePackagesAndRefs(ctx context.Context, tw *transactionWrapper, uploadID int, packages []Package, references []Reference) error {
	var packageRows []*sqlf.Query
	for _, p := range packages {
		packageRows = append(packageRows, sqlf.Sprintf("(%s, %s, %s)", p.Scheme, p.Name, p.Version))
	}
	query1 := `INSERT INTO lsif_packages (scheme, name, version) VALUES %s`
	if _, err := tw.exec(ctx, sqlf.Sprintf(query1, sqlf.Join(packageRows, ","))); err != nil {
		return err
	}

	var referenceRows []*sqlf.Query
	for _, r := range references {
		// TODO - filter
		referenceRows = append(referenceRows, sqlf.Sprintf("(%s, %s, %s, %s)", r.Scheme, r.Name, r.Version, ""))
	}
	query2 := `INSERT INTO lsif_references (scheme, name, version, filter) VALUES %s`
	if _, err := tw.exec(ctx, sqlf.Sprintf(query2, sqlf.Join(referenceRows, ","))); err != nil {
		return err
	}

	return nil
}

func (db *dbImpl) UpdateCommits(ctx context.Context, repositoryID int, commits map[string][]string) error {
	var rows []*sqlf.Query
	for commit, parents := range commits {
		for _, parent := range parents {
			rows = append(rows, sqlf.Sprintf("(%d, %s, %s)", repositoryID, commit, parent))
		}

		if len(parents) == 0 {
			rows = append(rows, sqlf.Sprintf("(%d, %s, NULL)", repositoryID, commit))
		}
	}

	query := `INSERT INTO lsif_commits (repository_id, "commit", parent_commit) VALUES %s`
	if err := db.exec(ctx, sqlf.Sprintf(query, sqlf.Join(rows, ","))); err != nil {
		return err
	}

	return nil
}

func (db *dbImpl) DeleteOverlappingDumps(ctx context.Context, tw *transactionWrapper, repositoryID int, commit, root, indexer string) error {
	query := `
		DELETE from lsif_uploads
		WHERE repository_id = %d AND commit = %s AND root = %s AND indexer = %s AND state='completed'
	`

	if _, err := tw.exec(ctx, sqlf.Sprintf(query, repositoryID, commit, root, indexer)); err != nil {
		return err
	}

	// TODO
	return nil
}
