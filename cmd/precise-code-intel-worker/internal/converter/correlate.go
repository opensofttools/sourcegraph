package converter

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
)

type CorrelationState struct {
	dumpRoot               string
	lsifVersion            string
	projectRoot            string
	unsupportedVertexes    idSet
	documentData           map[string]DocumentData
	rangeData              map[string]RangeData
	resultSetData          map[string]ResultSetData
	definitionData         map[string]defaultIDSetMap
	referenceData          map[string]defaultIDSetMap
	hoverData              map[string]string
	monikerData            map[string]MonikerData
	packageInformationData map[string]PackageInformationData
	nextData               map[string]string
	importedMonikers       idSet
	exportedMonikers       idSet
	linkedMonikers         disjointIDSet
	linkedReferenceResults disjointIDSet
}

func newCorrelationState(dumpRoot string) *CorrelationState {
	return &CorrelationState{
		dumpRoot:               dumpRoot,
		unsupportedVertexes:    newIDSet(),
		documentData:           map[string]DocumentData{},
		rangeData:              map[string]RangeData{},
		resultSetData:          map[string]ResultSetData{},
		definitionData:         map[string]defaultIDSetMap{},
		referenceData:          map[string]defaultIDSetMap{},
		hoverData:              map[string]string{},
		monikerData:            map[string]MonikerData{},
		packageInformationData: map[string]PackageInformationData{},
		nextData:               map[string]string{},
		importedMonikers:       newIDSet(),
		exportedMonikers:       newIDSet(),
		linkedMonikers:         newDisjointIDSet(),
		linkedReferenceResults: newDisjointIDSet(),
	}
}

var ErrMissingMetaData = errors.New("no metadata defined")

type ErrMalformedDump struct {
	ID         string
	References string
	Kinds      []string
}

func (e ErrMalformedDump) Error() string {
	// TODO
	return fmt.Sprintf("oh geesh")
}

func malformedDump(id, references string, kinds ...string) error {
	return ErrMalformedDump{
		ID:         id,
		References: references,
		Kinds:      kinds,
	}
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

	if cx.lsifVersion == "" {
		return nil, fmt.Errorf("no metadata defined")
	}

	return cx, nil
}

func correlateElement(state *CorrelationState, element Element) error {
	switch element.Type {
	case "vertex":
		return correlateVertex(state, element)
	case "edge":
		return correlateEdge(state, element)
	}

	return fmt.Errorf("unknown elemeent type %s", element.Type)
}

func correlateVertex(state *CorrelationState, element Element) error {
	handler, ok := vertexHandlers[element.Label]
	if !ok {
		state.unsupportedVertexes.add(element.ID)
		return nil
	}

	return handler(state, element)
}

var vertexHandlers = map[string]func(c *CorrelationState, element Element) error{
	"metaData":           correlateMetaData,
	"document":           correlateDocument,
	"range":              correlateRange,
	"resultSet":          correlateResultSet,
	"definitionResult":   correlateDefinitionResult,
	"referenceResult":    correlateReferenceResult,
	"hoverResult":        correlateHoverResult,
	"moniker":            correlateMoniker,
	"packageInformation": correlatePackageInformation,
}

func correlateEdge(state *CorrelationState, element Element) error {
	handler, ok := edgeHandlers[element.Label]
	if !ok {
		return nil
	}

	edge, err := unmarshalEdge(element)
	if err != nil {
		return err
	}

	return handler(state, element.ID, edge)
}

var edgeHandlers = map[string]func(c *CorrelationState, id string, edge Edge) error{
	"contains":                correlateContainsEdge,
	"next":                    correlateNextEdge,
	"item":                    correlateItemEdge,
	"textDocument_definition": correlateTextDocumentDefinitionEdge,
	"textDocument_references": correlateTextDocumentReferencesEdge,
	"textDocument_hover":      correlateTextDocumentHoverEdge,
	"moniker":                 correlateMonikerEdge,
	"nextMoniker":             correlateNextMonikerEdge,
	"packageInformation":      correlatePackageInformationEdge,
}

func correlateMetaData(c *CorrelationState, element Element) error {
	payload, err := unmarshalMetaData(element, c.dumpRoot)
	c.lsifVersion = payload.Version
	c.projectRoot = payload.ProjectRoot
	return err
}

func correlateDocument(c *CorrelationState, element Element) error {
	if c.projectRoot == "" {
		return ErrMissingMetaData
	}

	payload, err := unmarshalDocumentData(element, c.projectRoot)
	c.documentData[element.ID] = payload
	return err
}

func correlateRange(c *CorrelationState, element Element) error {
	payload, err := unmarshalRangeData(element)
	c.rangeData[element.ID] = payload
	return err
}

func correlateResultSet(c *CorrelationState, element Element) error {
	payload, err := unmarshalResultSetData(element)
	c.resultSetData[element.ID] = payload
	return err
}

func correlateDefinitionResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalDefinitionResultData(element)
	c.definitionData[element.ID] = payload
	return err
}

func correlateReferenceResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalReferenceResultData(element)
	c.referenceData[element.ID] = payload
	return err
}

func correlateHoverResult(c *CorrelationState, element Element) error {
	payload, err := unmarshalHoverData(element)
	c.hoverData[element.ID] = payload.Result
	return err
}

func correlateMoniker(c *CorrelationState, element Element) error {
	payload, err := unmarshalMonikerData(element)
	c.monikerData[element.ID] = payload
	return err
}

func correlatePackageInformation(c *CorrelationState, element Element) error {
	payload, err := unmarshalPackageInformationData(element)
	c.packageInformationData[element.ID] = payload
	return err
}

func correlateContainsEdge(c *CorrelationState, id string, edge Edge) error {
	doc, ok := c.documentData[edge.OutV]
	if !ok {
		// Do not track project contains
		return nil
	}

	for _, inV := range edge.InVs {
		if _, ok := c.rangeData[inV]; !ok {
			return malformedDump(id, edge.InV, "range")
		}
		doc.Contains.add(inV)
	}
	return nil
}

func correlateNextEdge(c *CorrelationState, id string, edge Edge) error {
	_, ok1 := c.rangeData[edge.OutV]
	_, ok2 := c.resultSetData[edge.OutV]
	if !ok1 && !ok2 {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	if _, ok := c.resultSetData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "resultSet")
	}

	c.nextData[edge.OutV] = edge.InV
	return nil
}

func correlateItemEdge(c *CorrelationState, id string, edge Edge) error {
	if documentMap, ok := c.definitionData[edge.OutV]; ok {
		for _, inV := range edge.InVs {
			if _, ok := c.rangeData[inV]; !ok {
				return malformedDump(id, edge.InV, "range")
			}
			documentMap[edge.Document].add(inV)
		}

		return nil
	}

	if documentMap, ok := c.referenceData[edge.OutV]; ok {
		for _, inV := range edge.InVs {
			if _, ok := c.referenceData[inV]; ok {
				c.linkedReferenceResults.union(edge.OutV, inV)
			} else {
				if _, ok = c.rangeData[inV]; !ok {
					return malformedDump(id, edge.InV, "range")
				}
				documentMap[edge.Document].add(inV)
			}
		}

		return nil
	}

	if !c.unsupportedVertexes.contains(edge.OutV) {
		return malformedDump(id, edge.OutV, "vertex")
	}

	// this.logger.debug("Skipping edge from an unsupported vertex")
	return nil
}

func correlateTextDocumentDefinitionEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.definitionData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "definitionResult")
	}

	if source, ok := c.rangeData[edge.OutV]; ok {
		c.rangeData[edge.OutV] = source.setDefinitionResultID(edge.InV)
	} else if source, ok := c.resultSetData[edge.OutV]; ok {
		c.resultSetData[edge.OutV] = source.setDefinitionResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}

	return nil
}

func correlateTextDocumentReferencesEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.referenceData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "referenceResult")
	}

	if source, ok := c.rangeData[edge.OutV]; ok {
		c.rangeData[edge.OutV] = source.setReferenceResultID(edge.InV)
	} else if source, ok := c.resultSetData[edge.OutV]; ok {
		c.resultSetData[edge.OutV] = source.setReferenceResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateTextDocumentHoverEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.hoverData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "hoverResult")
	}

	if source, ok := c.rangeData[edge.OutV]; ok {
		c.rangeData[edge.OutV] = source.setHoverResultID(edge.InV)
	} else if source, ok := c.resultSetData[edge.OutV]; ok {
		c.resultSetData[edge.OutV] = source.setHoverResultID(edge.InV)
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateMonikerEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.monikerData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "moniker")
	}

	if source, ok := c.rangeData[edge.OutV]; ok {
		c.rangeData[edge.OutV] = source.setMonikerIDs(newSingletonIDSet(edge.InV))
	} else if source, ok := c.resultSetData[edge.OutV]; ok {
		c.resultSetData[edge.OutV] = source.setMonikerIDs(newSingletonIDSet(edge.InV))
	} else {
		return malformedDump(id, edge.OutV, "range", "resultSet")
	}
	return nil
}

func correlateNextMonikerEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.monikerData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "moniker")
	}
	if _, ok := c.monikerData[edge.OutV]; !ok {
		return malformedDump(id, edge.OutV, "moniker")
	}

	c.linkedMonikers.union(edge.InV, edge.OutV)
	return nil
}

func correlatePackageInformationEdge(c *CorrelationState, id string, edge Edge) error {
	if _, ok := c.packageInformationData[edge.InV]; !ok {
		return malformedDump(id, edge.InV, "packageInformation")
	}

	source, ok := c.monikerData[edge.OutV]
	if !ok {
		return malformedDump(id, edge.OutV, "moniker")
	}

	switch source.Kind {
	case "import":
		c.importedMonikers.add(edge.OutV)
	case "export":
		c.exportedMonikers.add(edge.OutV)
	}

	c.monikerData[edge.OutV] = source.setPackageInformationID(edge.InV)
	return nil
}
