package converter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type ErrMalformedDump struct {
	ID         string
	References string
	Kind       string // TODO - make list instead
}

func (e ErrMalformedDump) Error() string {
	return fmt.Sprintf("oh geesh")
}

type Correlator struct {
	dumpRoot    string
	LSIFVersion string
	ProjectRoot string
	// TODO - gotta make all these top level things
	UnsupportedVertexes    IDSet
	DocumentPaths          map[string]string
	ContainsData           map[string]IDSet
	RangeData              map[string]RangeData
	ResultSetData          map[string]ResultSetData
	DefinitionData         map[string]map[string][]string
	ReferenceData          map[string]map[string][]string
	HoverData              map[string]string
	MonikerData            map[string]MonikerData
	PackageInformationData map[string]PackageInformationData
	NextData               map[string]string
	ImportedMonikers       IDSet
	ExportedMonikers       IDSet
	LinkedMonikers         DisjointSet
	LinkedReferenceResults DisjointSet
}

func New(dumpRoot string) *Correlator {
	return &Correlator{
		dumpRoot: dumpRoot,
	}
}

func (c *Correlator) Insert(element Element) error {
	vertexHandlers := map[string]func(element Element) error{
		"metaData":           c.handleMetaData,
		"document":           c.handleDocument,
		"range":              c.handleRange,
		"resultSet":          c.handleResultSet,
		"definitionResult":   c.handleDefinitionResult,
		"referenceResult":    c.handleReferenceResult,
		"hoverResult":        c.handleHoverResult,
		"moniker":            c.handleMoniker,
		"packageInformation": c.handlePackageInformation,
	}

	edgeHandlers := map[string]func(id string, edge Edge) error{
		"contains":                c.handleContainsEdge,
		"next":                    c.handleNextEdge,
		"item":                    c.handleItemEdge,
		"textDocument_definition": c.handleTextDocumentDefinitionEdge,
		"textDocument_references": c.handleTextDocumentReferencesEdge,
		"textDocument_hover":      c.handleTextDocumentHoverEdge,
		"moniker":                 c.handleMonikerEdge,
		"nextMoniker":             c.handleNextMonikerEdge,
		"packageInformation":      c.handlePackageInformationEdge,
	}

	if element.Type() == "vertex" {
		if handler, ok := vertexHandlers[element.Label()]; ok {
			return handler(element)
		}

		c.UnsupportedVertexes.Add(element.ID())
	} else if element.Type() == "edge" {
		edge, err := unmarshalEdge(element)
		if err != nil {
			return err
		}

		if handler, ok := edgeHandlers[element.Label()]; ok {
			return handler(element.ID(), edge)
		}
	}
	return nil
}

func (c *Correlator) handleMetaData(element Element) error {
	var payload struct {
		Version     string `json:"version"`
		ProjectRoot string `json:"projectRoot"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	c.LSIFVersion = payload.Version
	if !strings.HasSuffix(payload.ProjectRoot, "/") {
		payload.ProjectRoot += "/"
	}
	c.ProjectRoot = payload.ProjectRoot

	// We assume that the project root in the LSIF dump is either:
	//
	//   (1) the root of the LSIF dump, or
	//   (2) the root of the repository
	//
	// These are the common cases and we don't explicitly support
	// anything else. Here we normalize to (1) by appending the dump
	// root if it's not already suffixed by it.

	if c.dumpRoot != "" && !strings.HasPrefix(c.ProjectRoot, c.dumpRoot) {
		c.ProjectRoot += "/" + c.dumpRoot
	}

	return nil
}

func (c *Correlator) handleDocument(element Element) error {
	var payload struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	if c.ProjectRoot == "" {
		return fmt.Errorf("no metadata defined")
	}

	if !strings.HasPrefix(payload.URI, c.ProjectRoot) {
		return fmt.Errorf("Not a realative path...? %s %s", payload.URI, c.ProjectRoot)
	}

	c.DocumentPaths[element.ID()] = payload.URI[len(c.ProjectRoot):]
	c.ContainsData[element.ID()] = newIDSet()
	return nil
}

func (c *Correlator) handleRange(element Element) error {
	type Position struct {
		Line      int `json:"line"`
		Character int `json:"character"`
	}

	var payload struct {
		Start Position `json:"start"`
		End   Position `json:"end"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	c.RangeData[element.ID()] = RangeData{
		StartLine:      payload.Start.Line,
		StartCharacter: payload.Start.Character,
		EndLine:        payload.End.Line,
		EndCharacter:   payload.End.Character,
		MonikerIDs:     newIDSet(),
	}
	return nil
}

func (c *Correlator) handleResultSet(element Element) error {
	c.ResultSetData[element.ID()] = ResultSetData{
		MonikerIDs: newIDSet(),
	}
	return nil
}

func (c *Correlator) handleDefinitionResult(element Element) error {
	c.DefinitionData[element.ID()] = map[string][]string{}
	return nil
}

func (c *Correlator) handleReferenceResult(element Element) error {
	c.ReferenceData[element.ID()] = map[string][]string{}
	return nil
}

func (c *Correlator) handleHoverResult(element Element) error {
	var payload struct {
		Result string `json:"result"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	c.HoverData[element.ID()] = normalizeHover(payload.Result)
	return nil
}

func (c *Correlator) handleMoniker(element Element) error {
	var payload struct {
		Kind       string `json:"kind"`
		Scheme     string `json:"scheme"`
		Identifier string `json:"identifier"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	if payload.Kind == "" {
		payload.Kind = "local"
	}

	c.MonikerData[element.ID()] = MonikerData{
		Kind:       payload.Kind,
		Scheme:     payload.Scheme,
		Identifier: payload.Identifier,
	}
	return nil
}

func (c *Correlator) handlePackageInformation(element Element) error {
	var payload struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return err
	}

	c.PackageInformationData[element.ID()] = PackageInformationData{
		Name:    payload.Name,
		Version: payload.Version,
	}
	return nil
}

func (c *Correlator) handleContainsEdge(id string, edge Edge) error {
	// Do not track project contains
	if _, ok := c.DocumentPaths[edge.outV]; !ok {
		return nil
	}

	set, ok := c.ContainsData[edge.outV]
	if !ok {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "contains"}
	}

	for _, inV := range edge.inVs {
		if _, ok := c.RangeData[inV]; !ok {
			return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "range"}
		}
		set.Add(inV)
	}
	return nil
}

func (c *Correlator) handleNextEdge(id string, edge Edge) error {
	_, ok1 := c.RangeData[edge.outV]
	_, ok2 := c.ResultSetData[edge.outV]
	if !ok1 && !ok2 {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "range/resultSet"}
	}

	if _, ok := c.ResultSetData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "resultSet"}
	}

	c.NextData[edge.outV] = edge.inVs[0]
	return nil
}

func (c *Correlator) handleItemEdge(id string, edge Edge) error {
	if documentMap, ok := c.DefinitionData[edge.outV]; ok {
		var rangeIDs []string
		for _, inV := range edge.inVs {
			if _, ok := c.RangeData[inV]; !ok {
				return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "range"}
			}
			rangeIDs = append(rangeIDs, inV)
		}

		documentMap[edge.document] = append(documentMap[edge.document], rangeIDs...)
		return nil
	}

	if documentMap, ok := c.ReferenceData[edge.outV]; ok {
		var rangeIDs []string
		for _, inV := range edge.inVs {
			if _, ok := c.ReferenceData[inV]; ok {
				c.LinkedReferenceResults.Union(edge.outV, inV)
			} else {
				if _, ok = c.RangeData[inV]; !ok {
					return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "range"}
				}
				rangeIDs = append(rangeIDs, inV)
			}
		}

		documentMap[edge.document] = append(documentMap[edge.document], rangeIDs...)
		return nil
	}

	if c.UnsupportedVertexes.Has(edge.outV) {
		// TODO - log ?
		// this.logger.debug("Skipping edge from an unsupported vertex")
		return nil
	}

	return ErrMalformedDump{ID: id, References: edge.outV, Kind: "vertex"}
}

func (c *Correlator) handleTextDocumentDefinitionEdge(id string, edge Edge) error {
	if _, ok := c.DefinitionData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "definitionResult"}
	}

	if source, ok := c.RangeData[edge.outV]; ok {
		c.RangeData[edge.outV] = source.setDefinitionResultID(edge.inVs[0])
	} else if source, ok := c.ResultSetData[edge.outV]; ok {
		c.ResultSetData[edge.outV] = source.setDefinitionResultID(edge.inVs[0])
	} else {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "range/resultSet"}
	}

	return nil
}

func (c *Correlator) handleTextDocumentReferencesEdge(id string, edge Edge) error {
	if _, ok := c.ReferenceData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "referenceResult"}
	}

	if source, ok := c.RangeData[edge.outV]; ok {
		c.RangeData[edge.outV] = source.setReferenceResultID(edge.inVs[0])
	} else if source, ok := c.ResultSetData[edge.outV]; ok {
		c.ResultSetData[edge.outV] = source.setReferenceResultID(edge.inVs[0])
	} else {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "range/resultSet"}
	}
	return nil
}

func (c *Correlator) handleTextDocumentHoverEdge(id string, edge Edge) error {
	if _, ok := c.HoverData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "hoverResult"}
	}

	if source, ok := c.RangeData[edge.outV]; ok {
		c.RangeData[edge.outV] = source.setHoverResultID(edge.inVs[0])
	} else if source, ok := c.ResultSetData[edge.outV]; ok {
		c.ResultSetData[edge.outV] = source.setHoverResultID(edge.inVs[0])
	} else {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "range/resultSet"}
	}
	return nil
}

func (c *Correlator) handleMonikerEdge(id string, edge Edge) error {
	if _, ok := c.MonikerData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "moniker"}
	}

	if source, ok := c.RangeData[edge.outV]; ok {
		c.RangeData[edge.outV] = source.setMonikerIDs(newIDSetFrom([]string{edge.inVs[0]}))
	} else if source, ok := c.ResultSetData[edge.outV]; ok {
		c.ResultSetData[edge.outV] = source.setMonikerIDs(newIDSetFrom([]string{edge.inVs[0]}))
	} else {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "range/resultSet"}
	}
	return nil
}

func (c *Correlator) handleNextMonikerEdge(id string, edge Edge) error {
	if _, ok := c.MonikerData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "moniker"}
	}

	if _, ok := c.MonikerData[edge.outV]; !ok {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "moniker"}
	}

	c.LinkedMonikers.Union(edge.inVs[0], edge.outV)
	return nil
}

func (c *Correlator) handlePackageInformationEdge(id string, edge Edge) error {
	if _, ok := c.PackageInformationData[edge.inVs[0]]; !ok {
		return ErrMalformedDump{ID: id, References: edge.inVs[0], Kind: "packageInformation"}
	}

	source, ok := c.MonikerData[edge.outV]
	if !ok {
		return ErrMalformedDump{ID: id, References: edge.outV, Kind: "moniker"}
	}

	c.MonikerData[edge.outV] = source.setPackageInformationID(edge.inVs[0])

	if source.Kind == "export" {
		c.ExportedMonikers.Add(edge.outV)
	}

	if source.Kind == "import" {
		c.ImportedMonikers.Add(edge.outV)
	}
	return nil
}
