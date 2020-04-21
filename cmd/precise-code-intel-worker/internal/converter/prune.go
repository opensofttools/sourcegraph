package converter

import "github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/existence"

func Prune(repositoryID int, commit, root string, cx *CorrelationState) error {
	var paths []string
	for _, doc := range cx.documentData {
		paths = append(paths, doc.URI)
	}

	ec, err := existence.NewExistenceChecker(repositoryID, commit, root, paths)
	if err != nil {
		return err
	}

	for documentID, doc := range cx.documentData {
		// Do not gather any document that is not within the dump root or does not exist
		// in git. If the path is outside of the dump root, then it will never be queried
		// as the current text document path and the dump root are compared to determine
		// which dump to open. If the path does not exist in git, it will also never be
		// queried.
		if !ec.ShouldInclude(doc.URI) {
			delete(cx.documentData, documentID)
		}
	}

	for _, documentRanges := range cx.definitionData {
		for documentID := range documentRanges {
			doc, ok := cx.documentData[documentID]
			if !ok || !ec.ShouldInclude(doc.URI) {
				// Skip pointing to locations that are not available in git. This can occur
				// with indexers that point to generated files or dependencies that are not
				// committed (e.g. node_modules). Keeping these in the dump can cause the
				// UI to redirect to a path that doesn't exist.
				delete(documentRanges, documentID)
			}
		}
	}

	for _, documentRanges := range cx.referenceData {
		for documentID := range documentRanges {
			doc, ok := cx.documentData[documentID]
			if !ok || !ec.ShouldInclude(doc.URI) {
				// Skip pointing to locations that are not available in git. This can occur
				// with indexers that point to generated files or dependencies that are not
				// committed (e.g. node_modules). Keeping these in the dump can cause the
				// UI to redirect to a path that doesn't exist.
				delete(documentRanges, documentID)
			}
		}
	}

	return nil
}
