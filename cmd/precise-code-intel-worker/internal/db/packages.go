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
	Identifiers []string
}

func (db *dbImpl) UpdatePackagesAndRefs(ctx context.Context, tw *transactionWrapper, uploadID int, packages []Package, references []Reference) error {
	var packageRows []*sqlf.Query
	for _, p := range packages {
		packageRows=append(packageRows,sqlf.Sprintf("(%s, %s, %s)", p.Scheme, p.Name, p.Version))
	}
	query1 := `INSERT INTO lsif_packages (scheme, name, version) VALUES %s`
	if _, err := tw.exec(ctx, sqlf.Sprintf(query1, sqlf.Join(packageRows, ",")));err != nil {
		return err
	}

	var referenceRows []*sqlf.Query
	for _, r := range references {
		// TODO - filter
		referenceRows=append(referenceRows,sqlf.Sprintf("(%s, %s, %s, %s)", r.Scheme, r.Name, r.Version, ""))
	}
	query2 := `INSERT INTO lsif_references (scheme, name, version, filter) VALUES %s`
	if _, err := tw.exec(ctx, sqlf.Sprintf(query2, sqlf.Join(packageRows, ",")));err != nil {
		return err
	}

	return nil
}
