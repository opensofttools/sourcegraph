package converter

func canonicalize(cx *CorrelationState) {
	// Determine if multiple documents are defined with the same URI. This happens in
	// some indexers (such as lsif-tsc) that index dependent projects into the same
	// dump as the target project. For each set of documents that share a path, we
	// choose one document to be the canonical representative and merge the contains,
	// definition, and reference data into the unique canonical document.
	mergeDocuments(cx)

	// Determine which reference results are linked together. Determine a canonical
	// reference result for each set so that we can remap all identifiers to the
	// chosen one.
	canonicalizeReferenceResults(cx)

	// // Collapse result sets data into the ranges that can reach them. The
	// // remainder of this function assumes that we can completely ignore
	// // the "next" edges coming from range data.
	canonicalizeResultSets(cx)
	canonicalizeRanges(cx)
}

// * Merge the data in the correlator of all documents that share the same path. This
// * function works by moving the contains, definition, and reference data keyed by a
// * document with a duplicate path into a canonical document with that path. The first
// * document inserted for a path is the canonical document for that path. This function
// * guarantees that duplicate document ids are removed from these maps.
func mergeDocuments(cx *CorrelationState) {
	canonicalDocumentsIDsByPath := map[string]string{}
	for documentID, doc := range cx.documentData {
		canonicalDocumentID, ok := canonicalDocumentsIDsByPath[doc.URI]
		if !ok {
			canonicalDocumentsIDsByPath[doc.URI] = documentID
			continue
		}

		for id := range cx.documentData[documentID].Contains {
			cx.documentData[canonicalDocumentID].Contains.add(id)
		}
		delete(cx.documentData, documentID)

		for _, rangeIDsByDocumentID := range cx.definitionData {
			if rangeIDs, ok := rangeIDsByDocumentID[documentID]; ok {
				if _, ok := rangeIDsByDocumentID[canonicalDocumentID]; !ok {
					rangeIDsByDocumentID[canonicalDocumentID] = newIDSet()
				}
				rangeIDsByDocumentID[canonicalDocumentID].addAll(rangeIDs)
				delete(rangeIDsByDocumentID, documentID)
			}
		}

		for _, rangeIDsByDocumentID := range cx.referenceData {
			if rangeIDs, ok := rangeIDsByDocumentID[documentID]; ok {
				if _, ok := rangeIDsByDocumentID[canonicalDocumentID]; !ok {
					rangeIDsByDocumentID[canonicalDocumentID] = newIDSet()
				}
				rangeIDsByDocumentID[canonicalDocumentID].addAll(rangeIDs)
				delete(rangeIDsByDocumentID, documentID)
			}
		}
	}
}

// * Determine which reference result sets are linked via item edges. Choose a canonical
// * reference result from each batch. Merge all data into the canonical result and remove
// * all non-canonical results from the correlator (note: this leave unlinked results alone).
// * Return a map from reference result identifier to the identifier of the canonical result.
func canonicalizeReferenceResults(cx *CorrelationState) {
	referenceResultIDToCanonicalReferenceResultIDs := map[string]string{}
	for referenceResultID := range cx.linkedReferenceResults {
		// Don't re-process the same set of linked reference results
		if _, ok := referenceResultIDToCanonicalReferenceResultIDs[referenceResultID]; ok {
			continue
		}

		// Find all reachable items and order them deterministically
		linkedIDs := cx.linkedReferenceResults.extractSet(referenceResultID)
		canonicalID, _ := linkedIDs.choose()
		canonicalReferenceResult, _ := cx.referenceData[canonicalID]

		for linkedID := range linkedIDs {
			// Link each id to its canonical representation. We do this for
			// the `linkedId === canonicalId` case so we can reliably detect
			// duplication at the start of this loop.

			referenceResultIDToCanonicalReferenceResultIDs[linkedID] = canonicalID
			if linkedID == canonicalID {
				continue
			}

			// If it's a different identifier, then normalize all data from the linked result
			// set into the canonical one.
			for documentID, rangeIDs := range cx.referenceData[linkedID] {
				if _, ok := canonicalReferenceResult[documentID]; !ok {
					canonicalReferenceResult[documentID] = newIDSet()
				}
				canonicalReferenceResult[documentID].addAll(rangeIDs)
			}
		}
	}

	for id, item := range cx.rangeData {
		cx.rangeData[id] = item.setReferenceResultID(referenceResultIDToCanonicalReferenceResultIDs[item.ReferenceResultID])
	}

	for id, item := range cx.resultSetData {
		cx.resultSetData[id] = item.setReferenceResultID(referenceResultIDToCanonicalReferenceResultIDs[item.ReferenceResultID])
	}

	var canonicalReferenceResultIDs map[string]struct{}
	for _, canonicalID := range referenceResultIDToCanonicalReferenceResultIDs {
		canonicalReferenceResultIDs[canonicalID] = struct{}{}
	}

	for referenceResultID := range referenceResultIDToCanonicalReferenceResultIDs {
		if _, ok := canonicalReferenceResultIDs[referenceResultID]; !ok {
			delete(cx.referenceData, referenceResultID)
		}
	}
}

func canonicalizeRanges(cx *CorrelationState) {
	for rangeID, rangeData := range cx.rangeData {
		if _, nextItem, ok := next(cx, rangeID); ok {
			rangeData = mergeNextRangeData(rangeData, nextItem)
		}

		cx.rangeData[rangeID] = rangeData.setMonikerIDs(gatherMonikers(cx, rangeData.MonikerIDs))
	}
}

func canonicalizeResultSets(cx *CorrelationState) {
	for resultSetID, resultSetData := range cx.resultSetData {
		canonicalizeResultSetData(cx, resultSetID, resultSetData)
	}

	for resultSetID, resultSetData := range cx.resultSetData {
		cx.resultSetData[resultSetID] = resultSetData.setMonikerIDs(gatherMonikers(cx, resultSetData.MonikerIDs))
	}
}

// TODO - try to do this in a different way
func canonicalizeResultSetData(cx *CorrelationState, id string, item ResultSetData) ResultSetData {
	if nextID, nextItem, ok := next(cx, id); ok {
		item = mergeNextResultSetData(item, canonicalizeResultSetData(cx, nextID, nextItem))
		cx.resultSetData[id] = item
		delete(cx.nextData, nextID)
	}

	return item
}

func mergeNextRangeData(item RangeData, nextItem ResultSetData) RangeData {
	if item.DefinitionResultID == "" {
		item = item.setDefinitionResultID(nextItem.DefinitionResultID)
	}
	if item.ReferenceResultID == "" {
		item = item.setReferenceResultID(nextItem.ReferenceResultID)
	}
	if item.HoverResultID == "" {
		item = item.setHoverResultID(nextItem.HoverResultID)
	}

	item.MonikerIDs.addAll(nextItem.MonikerIDs)
	return item
}

func mergeNextResultSetData(item, nextItem ResultSetData) ResultSetData {
	if item.DefinitionResultID == "" {
		item = item.setDefinitionResultID(nextItem.DefinitionResultID)
	}
	if item.ReferenceResultID == "" {
		item = item.setReferenceResultID(nextItem.ReferenceResultID)
	}
	if item.HoverResultID == "" {
		item = item.setHoverResultID(nextItem.HoverResultID)
	}

	item.MonikerIDs.addAll(nextItem.MonikerIDs)
	return item
}

func gatherMonikers(cx *CorrelationState, source idSet) idSet {
	monikers := newIDSet()
	if canonicalID, ok := source.choose(); ok {
		for id := range cx.linkedMonikers.extractSet(canonicalID) {
			data, ok := cx.monikerData[id]
			if !ok {
				// TODO - error
			}
			if data.Kind != "local" {
				monikers.add(id)
			}
		}
	}

	return monikers
}

func next(cx *CorrelationState, id string) (string, ResultSetData, bool) {
	nextID, ok := cx.nextData[id]
	if ok {
		nextItem, _ := cx.resultSetData[nextID]
		return nextID, nextItem, true
	}

	return "", ResultSetData{}, false
}
