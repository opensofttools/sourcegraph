package converter

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math"
	"os"
	"sort"

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

	if cx.LSIFVersion == "" {
		return nil, nil, fmt.Errorf("no metadata defined")
	}

	// Determine if multiple documents are defined with the same URI. This happens in
	// some indexers (such as lsif-tsc) that index dependent projects into the same
	// dump as the target project. For each set of documents that share a path, we
	// choose one document to be the canonical representative and merge the contains,
	// definition, and reference data into the unique canonical document.
	mergeDocuments(cx)

	// Determine which reference results are linked together. Determine a canonical
	// reference result for each set so that we can remap all identifiers to the
	// chosen one.
	canonicalReferenceResultIDs := canonicalizeReferenceResults(cx)

	// TODO - path existence stuff
	// TODO

	// Calculate the number of result chunks that we'll attempt to populate
	numResults := len(cx.DefinitionData) + len(cx.ReferenceData)
	numResultChunks := int(math.Min(MaxNumResultChunks, math.Max(1, math.Floor(float64(numResults)/ResultsPerResultChunk))))

	if err := populateMetadataTable(db, cx, numResultChunks); err != nil {
		return nil, nil, err
	}

	if err := populateDocumentsTable(db, cx, canonicalReferenceResultIDs); err != nil {
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
	for id := range cx.ExportedMonikers {
		source := cx.MonikerData[id]
		packageInfo := cx.PackageInformationData[source.PackageInformationID]
		packages = append(packages, Package{
			Scheme:  source.Scheme,
			Name:    packageInfo.Name,
			Version: packageInfo.Version,
		})
	}

	// TODO - flatten
	references := map[string]Reference{}
	for id := range cx.ImportedMonikers {
		source := cx.MonikerData[id]
		packageInfo := cx.PackageInformationData[source.PackageInformationID]
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

func correlate(filename, root string) (*Correlator, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}

	cx := New(root)
	scanner := bufio.NewScanner(gzipReader)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		element, err := UnmarshalElement(scanner.Bytes())
		if err != nil {
			return nil, err
		}

		if err := cx.Insert(element); err != nil {
			return nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return cx, nil
}

// * Merge the data in the correlator of all documents that share the same path. This
// * function works by moving the contains, definition, and reference data keyed by a
// * document with a duplicate path into a canonical document with that path. The first
// * document inserted for a path is the canonical document for that path. This function
// * guarantees that duplicate document ids are removed from these maps.
func mergeDocuments(cx *Correlator) {
	uriMap := map[string]string{}
	for id, path := range cx.DocumentPaths {
		canonicalID, ok := uriMap[path]
		if !ok {
			uriMap[path] = id
			continue
		}

		mergeContains(cx, id, canonicalID)
		mergeDefinitions(cx, id, canonicalID)
		mergeReferences(cx, id, canonicalID)
		delete(cx.DocumentPaths, id)
	}
}

// * Move the contains data for document `id` into the contains data of document
// * `canonicalId`, then delete the reference to document `id.`
func mergeContains(cx *Correlator, id, canonicalID string) {
	contains, ok := cx.ContainsData[id]
	if !ok {
		// TODO - error
	}

	canonicalContains, ok := cx.ContainsData[canonicalID]
	if !ok {
		// TODO - error
	}

	for id := range contains {
		canonicalContains.Add(id)
	}

	// Do not keep refs to document id we're removing
	delete(cx.ContainsData, id)
}

// * Move the definition or reference data for document `id` into the definition or
// * reference data of document `canonicalId`, then delete the reference to document
// * `id.`
func mergeDefinitions(cx *Correlator, id, canonicalID string) {
	for _, v := range cx.DefinitionData {
		if data, ok := v[id]; ok {
			for _, id := range data {
				v[canonicalID] = append(v[canonicalID], id)
			}

			// Do not keep refs to document id we're removing
			delete(v, id)
		}
	}
}

func mergeReferences(cx *Correlator, id, canonicalID string) {
	for _, v := range cx.ReferenceData {
		if data, ok := v[id]; ok {
			for _, id := range data {
				v[canonicalID] = append(v[canonicalID], id)
			}

			// Do not keep refs to document id we're removing
			delete(v, id)
		}
	}
}

// * Determine which reference result sets are linked via item edges. Choose a canonical
// * reference result from each batch. Merge all data into the canonical result and remove
// * all non-canonical results from the correlator (note: this leave unlinked results alone).
// * Return a map from reference result identifier to the identifier of the canonical result.
func canonicalizeReferenceResults(cx *Correlator) map[string]string {
	canonicalReferenceResultIDs := map[string]string{}

	for referenceResultID := range cx.LinkedReferenceResults {
		// Don't re-process the same set of linked reference results
		if _, ok := canonicalReferenceResultIDs[referenceResultID]; ok {
			continue
		}

		// Find all reachable items and order them deterministically
		linkedIDs := cx.LinkedReferenceResults.ExtractSet(referenceResultID).Keys()
		sort.Strings(linkedIDs)

		// Choose arbitrary canonical id
		canonicalID := linkedIDs[0]
		canonicalReferenceResult, ok := cx.ReferenceData[canonicalID]
		if !ok {
			// TODO - error
		}

		for _, linkedID := range linkedIDs {
			// Link each id to its canonical representation. We do this for
			// the `linkedId === canonicalId` case so we can reliably detect
			// duplication at the start of this loop.

			canonicalReferenceResultIDs[linkedID] = canonicalID
			if linkedID != canonicalID {
				// If it's a different identifier, then normalize all data from the linked result
				// set into the canonical one.
				for documentID, rangeIDs := range cx.ReferenceData[linkedID] {
					canonicalReferenceResult[documentID] = append(canonicalReferenceResult[documentID], rangeIDs...)
				}
			}
		}
	}

	// Remove all non-canonical but linked result sets
	var xxx map[string]struct{} // TODO - use id set
	for _, v := range canonicalReferenceResultIDs {
		xxx[v] = struct{}{}
	}
	for k := range canonicalReferenceResultIDs {
		if _, ok := xxx[k]; !ok {
			delete(cx.ReferenceData, k)
		}
	}

	return canonicalReferenceResultIDs
}

func populateMetadataTable(db *sqlx.DB, cx *Correlator, numResultChunks int) error {
	// TODO - need to write schema?
	query := `
		INSERT INTO metadata (id, lsifVersion, sourcegraphVersion, numResultChunks)
		VALUES (1, $1, $2, $3)
	`

	_, err := db.Exec(query, cx.LSIFVersion, InternalVersion, numResultChunks)
	return err
}

func populateDocumentsTable(db *sqlx.DB, cx *Correlator, canonicalReferenceResultIDs map[string]string) error {
	// Collapse result sets data into the ranges that can reach them. The
	// remainder of this function assumes that we can completely ignore
	// the "next" edges coming from range data.
	for rangeID, r := range cx.RangeData {
		canonicalizeItem(cx, canonicalReferenceResultIDs, rangeID, r)
	}

	// Gather and insert document data that includes the ranges contained in the document,
	// any associated hover data, and any associated moniker data/package information.
	// Each range also has identifiers that correlate to a definition or reference result
	// which can be found in a result chunk, created in the next step.

	for documentID, documentPath := range cx.DocumentPaths {
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
		document := gatherDocument(cx, documentID, documentPath)

		data, err := gzipJSON(document)
		if err != nil {
			// TODO
		}

		// TODO - need to write schema?
		// Encode and insert document record
		query := `
			INSERT INTO documents (path, data)
			VALUES (1, $1)
		`
		if _, err := db.Exec(query, documentPath, data); err != nil {
			return err
		}
	}

	return nil
}

type ResultChunk struct {
	Paths              map[string]string `json:"paths"`
	DocumentIDRangeIDs map[string][]struct {
		DocumentID string `json:"documentId"`
		RangeID    string `json:"rangeId"`
	} `json:"documentIdRangeIds"`
}

func populateResultChunksTable(db *sqlx.DB, cx *Correlator, numResultChunks int) error {
	resultChunks := make([]ResultChunk, numResultChunks)

	chunkResults := func(data map[string]map[string][]string) {
		for id, documentRanges := range data {
			var flattenedRangeList []struct {
				documentID   string
				rangeID      string
				documentPath string
			}

			for documentID, rangeIDs := range documentRanges {
				documentPath, ok := cx.DocumentPaths[documentID]
				if !ok {
					// TODO
				}

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
				for _, rangeID := range rangeIDs {
					things = append(things, struct {
						documentID   string
						rangeID      string
						documentPath string
					}{documentID, rangeID, documentPath})
				}

				flattenedRangeList = append(flattenedRangeList, things...)
			}

			resultChunk := resultChunks[hashKey(id, len(resultChunks))]

			var pathless []struct {
				DocumentID string `json:"documentId"`
				RangeID    string `json:"rangeId"`
			}
			for _, x := range flattenedRangeList {
				path, ok := cx.DocumentPaths[x.documentID]
				if !ok {
					// TODO
				}

				resultChunk.Paths[x.documentID] = path

				pathless = append(pathless, struct {
					DocumentID string `json:"documentId"`
					RangeID    string `json:"rangeId"`
				}{x.documentID, x.rangeID})
			}

			resultChunk.DocumentIDRangeIDs[id] = pathless
		}
	}

	chunkResults(cx.DefinitionData)
	chunkResults(cx.ReferenceData)

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

func populateDefinitionsAndReferencesTables(db *sqlx.DB, cx *Correlator) error {
	// Determine the set of monikers that are attached to a definition or a
	// reference result. Correlating information in this way has two benefits:
	//   (1) it reduces duplicates in the definitions and references tables
	//   (2) it stop us from re-iterating over the range data of the entire
	//       LSIF dump, which is by far the largest proportion of data.

	definitionMonikers := map[string]IDSet{}
	referenceMonikers := map[string]IDSet{}

	for _, r := range cx.RangeData {
		if len(r.MonikerIDs) == 0 {
			continue
		}

		if r.DefinitionResultID != "" {
			s, ok := definitionMonikers[r.DefinitionResultID]
			if !ok {
				s = IDSet(map[string]struct{}{})
				definitionMonikers[r.DefinitionResultID] = s
			}

			for id := range r.MonikerIDs {
				s.Add(id)
			}
		}

		if r.ReferenceResultID != "" {
			s, ok := referenceMonikers[r.ReferenceResultID]
			if !ok {
				s = IDSet(map[string]struct{}{})
				referenceMonikers[r.ReferenceResultID] = s
			}

			for id := range r.MonikerIDs {
				s.Add(id)
			}
		}
	}

	insertMonikerRanges := func(data map[string]map[string][]string, monikers map[string]IDSet, tableName string) {
		for id, documentRanges := range data {
			// Get monikers. Nothing to insert if we don't have any.
			monikerIDs, ok := monikers[id]
			if !ok {
				continue
			}

			// Correlate each moniker with the document/range pairs stored in
			// the result set provided by the data argument of this function.

			for monikerID := range monikerIDs {
				moniker, ok := cx.MonikerData[monikerID]
				if !ok {
					// TODO
				}

				for documentID, rangeIDs := range documentRanges {
					documentPath, ok := cx.DocumentPaths[documentID]
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

					for _, id := range rangeIDs {
						r, ok := cx.RangeData[id]
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

						_, err := db.Exec(query, moniker.Scheme, moniker.Identifier, documentPath, r.StartLine, r.StartCharacter, r.EndLine, r.EndCharacter)
						if err != nil {
							// TODO
						}
					}
				}
			}
		}
	}

	insertMonikerRanges(cx.DefinitionData, definitionMonikers, "definitions")
	insertMonikerRanges(cx.ReferenceData, referenceMonikers, "references")
	return nil
}

// TODO - also applies to result set data :/
func canonicalizeItem(cx *Correlator, canonicalReferenceResultIDs map[string]string, id string, item RangeData) {
	monikers := IDSet(map[string]struct{}{})
	if len(item.MonikerIDs) > 0 {
		// Find arbitrary moniker attached to item
		candidateMoniker := item.MonikerIDs.Keys()[0]

		// Get all monikers reachable from this one
		for id := range cx.LinkedMonikers.ExtractSet(candidateMoniker) {
			data, ok := cx.MonikerData[id]
			if !ok {
				// TODO - error
			}
			if data.Kind != "local" {
				monikers.Add(id)
			}
		}
	}

	nextID, ok := cx.NextData[id]
	if ok {
		// If we have a next edge to a result set, get it and canonicalize it first. This
		// will recursively look at any result that that it can reach that hasn't yet been
		// canonicalized.

		nextItem, ok := cx.ResultSetData[nextID]
		if !ok {
			// TODO - error
		}
		// TODO - need to recurse
		// canonicalizeItem(cx, canonicalReferenceResultIDs, nextID, nextItem)

		// Add each moniker of the next set to this item
		for monikerID := range nextItem.MonikerIDs {
			monikers.Add(monikerID)
		}

		// If we do not have a definition, reference, or hover result, take the result
		// value from the next item.

		if item.DefinitionResultID == "" {
			item.DefinitionResultID = nextItem.DefinitionResultID // TODO - update
		}

		if item.ReferenceResultID == "" {
			item.ReferenceResultID = nextItem.ReferenceResultID // TODO - update
		}

		if item.HoverResultID == "" {
			item.HoverResultID = nextItem.HoverResultID // TODO - update
		}
	}

	if item.ReferenceResultID != "" {
		if x, ok := canonicalReferenceResultIDs[item.ReferenceResultID]; ok {
			// If there is a canonical version of this reference result, use that instead
			item.ReferenceResultID = x // TODO - update
		}
	}

	item.MonikerIDs = monikers // TODO - update
	delete(cx.NextData, id)
}

type DocumentData struct {
	Ranges             map[string]RangeData              `json:"ranges"`
	HoverResults       map[string]string                 `json:"hoverResults"`
	Monikers           map[string]MonikerData            `json:"monikers"`
	PackageInformation map[string]PackageInformationData `json:"packageInformation"`
}

func gatherDocument(cx *Correlator, documentID, path string) DocumentData {
	document := DocumentData{
		Ranges:             map[string]RangeData{},
		HoverResults:       map[string]string{},
		Monikers:           map[string]MonikerData{},
		PackageInformation: map[string]PackageInformationData{},
	}

	addHover := func(id string) {
		if id == "" {
			return
		}

		if _, ok := document.HoverResults[id]; ok {
			return
		}

		document.HoverResults[id] = cx.HoverData[id]
	}

	addPackageInformation := func(id string) {
		if id == "" {
			return
		}
		if _, ok := document.PackageInformation[id]; ok {
			return
		}

		document.PackageInformation[id] = cx.PackageInformationData[id]
	}

	addMoniker := func(id string) {
		if _, ok := document.Monikers[id]; ok {
			return
		}

		moniker := cx.MonikerData[id]
		document.Monikers[id] = moniker
		addPackageInformation(moniker.PackageInformationID)
	}

	for id := range cx.ContainsData[documentID] {
		r, ok := cx.RangeData[id]
		if !ok {
			// TODO
		}

		addHover(r.HoverResultID)

		for monikerID := range r.MonikerIDs {
			addMoniker(monikerID)
		}
		document.Ranges[id] = r
	}

	return document
}
