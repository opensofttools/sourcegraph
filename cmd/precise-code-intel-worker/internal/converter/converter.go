package converter

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math"
	"os"

	"github.com/jmoiron/sqlx"
)

const MaxNumResultChunks = 1000
const ResultsPerResultChunk = 500
const InternalVersion = "0.1.0"

func Convert(filename, root string) (_ []Package, _ []Reference, err error) {
	db, err := sqlx.Open("sqlite3_with_pcre", filename)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		// TODO - close anyway, combine errors
		if err != nil {
			err = db.Close()
		}
	}()

	cx, err := correlate(filename, root)
	if err != nil {
		return nil, nil, err
	}

	if cx.lsifVersion == "" {
		return nil, nil, fmt.Errorf("no metadata defined")
	}

	canonicalize(cx)

	// TODO - path existence stuff
	// TODO

	// Calculate the number of result chunks that we'll attempt to populate
	numResults := len(cx.definitionData) + len(cx.referenceData)
	numResultChunks := int(math.Min(MaxNumResultChunks, math.Max(1, math.Floor(float64(numResults)/ResultsPerResultChunk))))

	if err := populateMetadataTable(db, cx, numResultChunks); err != nil {
		return nil, nil, err
	}

	if err := populateDocumentsTable(db, cx); err != nil {
		return nil, nil, err
	}

	if err := populateResultChunksTable(db, cx, numResultChunks); err != nil {
		return nil, nil, err
	}

	if err := populateDefinitionsAndReferencesTables(db, cx); err != nil {
		return nil, nil, err
	}

	// TODO - dedpulicate
	var packages []Package
	for id := range cx.exportedMonikers {
		source := cx.monikerData[id]
		packageInfo := cx.packageInformationData[source.PackageInformationID]
		packages = append(packages, Package{
			Scheme:  source.Scheme,
			Name:    packageInfo.Name,
			Version: packageInfo.Version,
		})
	}

	// TODO - flatten
	references := map[string]Reference{}
	for id := range cx.importedMonikers {
		source := cx.monikerData[id]
		packageInfo := cx.packageInformationData[source.PackageInformationID]
		key := fmt.Sprintf("%s:%s:%s", source.Scheme, packageInfo.Name, packageInfo.Version)
		references[key] = Reference{
			Scheme:      source.Scheme,
			Name:        packageInfo.Name,
			Version:     packageInfo.Version,
			Identifiers: append(references[key].Identifiers, source.Identifier),
		}
	}

	var refs []Reference
	for _, ref := range references {
		refs = append(refs, ref)
	}

	return packages, refs, nil
}

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

func correlate(filename, root string) (*CorrelationState, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	cx := newCorrelationState(root)
	scanner := bufio.NewScanner(gzipReader)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		element, err := unmarshalElement(scanner.Bytes())
		if err != nil {
			return nil, err
		}

		if err := correlateElement(cx, element); err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return cx, nil
}

//
//
//

func populateMetadataTable(db *sqlx.DB, cx *CorrelationState, numResultChunks int) error {
	// TODO - need to write schema?
	query := `
		INSERT INTO metadata (id, lsifVersion, sourcegraphVersion, numResultChunks)
		VALUES (1, $1, $2, $3)
	`

	_, err := db.Exec(query, cx.lsifVersion, InternalVersion, numResultChunks)
	return err
}

//
//
//
func populateDocumentsTable(db *sqlx.DB, cx *CorrelationState) error {
	// Gather and insert document data that includes the ranges contained in the document,
	// any associated hover data, and any associated moniker data/package information.
	// Each range also has identifiers that correlate to a definition or reference result
	// which can be found in a result chunk, created in the next step.

	for documentID, doc := range cx.documentData {
		// Do not gather any document that is not within the dump root or does not exist
		// in git. If the path is outside of the dump root, then it will never be queried
		// as the current text document path and the dump root are compared to determine
		// which dump to open. If the path does not exist in git, it will also never be
		// queried.
		// if (!pathExistenceChecker.shouldIncludePath(documentPath)) {
		//     continue
		// }
		// TODO

		// Create document record from the correlated information. This will also insert
		// external definitions and references into the maps initialized above, which are
		// inserted into the definitions and references table, respectively, below.
		document := gatherDocument(cx, documentID, doc.URI)

		data, err := gzipJSON(document)
		if err != nil {
			// TODO
		}

		// TODO - need to write schema?
		// Encode and insert document record
		query := `
			INSERT INTO documents (path, data)
			VALUES ($1, $2)
		`
		if _, err := db.Exec(query, doc.URI, data); err != nil {
			return err
		}
	}

	return nil
}

//
//

type DocumentDatas struct {
	Ranges             map[string]RangeData              `json:"ranges"`
	HoverResults       map[string]string                 `json:"hoverResults"`
	Monikers           map[string]MonikerData            `json:"monikers"`
	PackageInformation map[string]PackageInformationData `json:"packageInformation"`
}

func gatherDocument(cx *CorrelationState, documentID, path string) DocumentDatas {
	document := DocumentDatas{
		Ranges:             map[string]RangeData{},
		HoverResults:       map[string]string{},
		Monikers:           map[string]MonikerData{},
		PackageInformation: map[string]PackageInformationData{},
	}

	for rangeID := range cx.documentData[documentID].Contains {
		r, _ := cx.rangeData[rangeID]
		document.Ranges[rangeID] = r

		if r.HoverResultID != "" {
			hoverData := cx.hoverData[r.HoverResultID]
			document.HoverResults[r.HoverResultID] = hoverData
		}

		for monikerID := range r.MonikerIDs {
			moniker := cx.monikerData[monikerID]
			document.Monikers[monikerID] = moniker

			if moniker.PackageInformationID != "" {
				packageInformation := cx.packageInformationData[moniker.PackageInformationID]
				document.PackageInformation[moniker.PackageInformationID] = packageInformation
			}
		}
	}

	return document
}

//
//
//

type ResultChunk struct {
	Paths              map[string]string `json:"paths"`
	DocumentIDRangeIDs map[string][]struct {
		DocumentID string `json:"documentId"`
		RangeID    string `json:"rangeId"`
	} `json:"documentIdRangeIds"`
}

func populateResultChunksTable(db *sqlx.DB, cx *CorrelationState, numResultChunks int) error {
	resultChunks := make([]ResultChunk, numResultChunks)

	chunkResults := func(data map[string]DefinitionResultData) {
		for id, documentRanges := range data {
			var flattenedRangeList []struct {
				documentID   string
				rangeID      string
				documentPath string
			}

			for documentID, rangeIDs := range documentRanges {
				doc, _ := cx.documentData[documentID]

				// Skip pointing to locations that are not available in git. This can occur
				// with indexers that point to generated files or dependencies that are not
				// committed (e.g. node_modules). Keeping these in the dump can cause the
				// UI to redirect to a path that doesn't exist.
				// TODO
				// if (!pathExistenceChecker.shouldIncludePath(documentPath, false)) {
				//     continue
				// }

				var things []struct {
					documentID   string
					rangeID      string
					documentPath string
				}
				for rangeID := range rangeIDs {
					things = append(things, struct {
						documentID   string
						rangeID      string
						documentPath string
					}{documentID, rangeID, doc.URI})
				}

				flattenedRangeList = append(flattenedRangeList, things...)
			}

			resultChunk := resultChunks[hashKey(id, len(resultChunks))]

			var pathless []struct {
				DocumentID string `json:"documentId"`
				RangeID    string `json:"rangeId"`
			}
			for _, x := range flattenedRangeList {
				doc, ok := cx.documentData[x.documentID]
				if !ok {
					// TODO
				}

				resultChunk.Paths[x.documentID] = doc.URI

				pathless = append(pathless, struct {
					DocumentID string `json:"documentId"`
					RangeID    string `json:"rangeId"`
				}{x.documentID, x.rangeID})
			}

			resultChunk.DocumentIDRangeIDs[id] = pathless
		}
	}

	chunkResults(cx.definitionData)
	// chunkResults(cx.referenceData)

	for id, resultChunk := range resultChunks {
		if len(resultChunk.Paths) == 0 && len(resultChunk.DocumentIDRangeIDs) == 0 {
			continue
		}

		data, err := gzipJSON(resultChunk)
		if err != nil {
			// TODO
		}

		// TODO - need to write schema?
		// Encode and insert document record
		query := `
			INSERT INTO resultChunks (path, data)
			VALUES (1, $1)
		`
		if _, err := db.Exec(query, id, data); err != nil {
			return err
		}
	}

	return nil
}

//
//
//

func populateDefinitionsAndReferencesTables(db *sqlx.DB, cx *CorrelationState) error {
	// Determine the set of monikers that are attached to a definition or a
	// reference result. Correlating information in this way has two benefits:
	//   (1) it reduces duplicates in the definitions and references tables
	//   (2) it stop us from re-iterating over the range data of the entire
	//       LSIF dump, which is by far the largest proportion of data.

	definitionMonikers := map[string]idSet{}
	referenceMonikers := map[string]idSet{}

	for _, r := range cx.rangeData {
		if len(r.MonikerIDs) == 0 {
			continue
		}

		if r.DefinitionResultID != "" {
			s, ok := definitionMonikers[r.DefinitionResultID]
			if !ok {
				s = newIDSet()
				definitionMonikers[r.DefinitionResultID] = s
			}

			for id := range r.MonikerIDs {
				s.add(id)
			}
		}

		if r.ReferenceResultID != "" {
			s, ok := referenceMonikers[r.ReferenceResultID]
			if !ok {
				s = newIDSet()
				referenceMonikers[r.ReferenceResultID] = s
			}

			for id := range r.MonikerIDs {
				s.add(id)
			}
		}
	}

	insertMonikerRanges := func(data map[string]DefinitionResultData, monikers map[string]idSet, tableName string) {
		for id, documentRanges := range data {
			// Get monikers. Nothing to insert if we don't have any.
			monikerIDs, ok := monikers[id]
			if !ok {
				continue
			}

			// Correlate each moniker with the document/range pairs stored in
			// the result set provided by the data argument of this function.

			for monikerID := range monikerIDs {
				moniker, ok := cx.monikerData[monikerID]
				if !ok {
					// TODO
				}

				for documentID, rangeIDs := range documentRanges {
					doc, ok := cx.documentData[documentID]
					if !ok {
						// TODO
					}

					// Skip definitions or references that point to a document that are not
					// present in the dump. Including this would cause a query that always
					// fails when it cannot resolve the missing document data.
					// TODO
					// if (!pathExistenceChecker.shouldIncludePath(documentPath)) {
					//     continue
					// }

					for id := range rangeIDs {
						r, ok := cx.rangeData[id]
						if !ok {
							// TODO
						}

						// moniker.Scheme
						// moniker.Identifier
						// documentPath
						// r...,

						query := "INSERT INTO " + tableName + `(
							scheme,
							identifier,
							documentPath,
							startLine,
							endLine,
							startCharacter,
							endCharacter
							) VALUES ($1, $2, $3, $4, $5, $6, $7)
						`

						// TODO - need to write schema?

						_, err := db.Exec(query, moniker.Scheme, moniker.Identifier, doc.URI, r.StartLine, r.StartCharacter, r.EndLine, r.EndCharacter)
						if err != nil {
							// TODO
						}
					}
				}
			}
		}
	}

	insertMonikerRanges(cx.definitionData, definitionMonikers, "definitions")
	// insertMonikerRanges(cx.referenceData, referenceMonikers, "references")
	return nil
}
