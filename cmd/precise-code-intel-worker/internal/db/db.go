package db

import (
	"context"
)

type DB interface {
	Dequeue(ctx context.Context) (Upload, TxCloser, bool, error)
	UpdatePackagesAndRefs(ctx context.Context, tw *transactionWrapper, uploadID int, packages []Package, referencess []Reference) error
	UpdateCommits(ctx context.Context, repositoryID int, commits map[string][]string) error
	DeleteOverlappingDumps(ctx context.Context, tw *transactionWrapper, repositoryID int, commit, root, indexer string) error
}
