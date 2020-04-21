package converter

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Element struct {
	ID    string `json:"id"` // TODO - strig or int
	Type  string `json:"type"`
	Label string `json:"label"`
	Raw   json.RawMessage
}

func unmarshalElement(Raw []byte) (payload Element, err error) {
	err = json.Unmarshal(Raw, &payload)
	payload.Raw = json.RawMessage(Raw)
	return
}

//
//

type Edge struct {
	OutV     string   `json:"outV"`
	InV      string   `json:"inV"`
	InVs     []string `json:"inVs"`
	Document string   `json:"document"`
}

func unmarshalEdge(element Element) (payload Edge, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	return
}

//
//

type MetaData struct {
	Version     string `json:"version"`
	ProjectRoot string `json:"projectRoot"`
}

func unmarshalMetaData(element Element, dumpRoot string) (payload MetaData, err error) {
	err = json.Unmarshal(element.Raw, &payload)

	// We assume that the project root in the LSIF dump is either:
	//
	//   (1) the root of the LSIF dump, or
	//   (2) the root of the repository
	//
	// These are the common cases and we don't explicitly support
	// anything else. Here we normalize to (1) by appending the dump
	// root if it's not already suffixed by it.

	if !strings.HasSuffix(payload.ProjectRoot, "/") {
		payload.ProjectRoot += "/"
	}

	if dumpRoot != "" && !strings.HasPrefix(payload.ProjectRoot, dumpRoot) {
		payload.ProjectRoot += "/" + dumpRoot
	}

	return
}

//
//

type DocumentData struct {
	URI      string `json:"uri"`
	Contains idSet
}

func unmarshalDocumentData(element Element, projectRoot string) (payload DocumentData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	if !strings.HasPrefix(payload.URI, projectRoot) {
		return DocumentData{}, fmt.Errorf("document URI %s is not relative to project root %s", payload.URI, projectRoot)
	}
	payload.URI = payload.URI[len(projectRoot):]
	payload.Contains = newIDSet()
	return
}

//
//

type RangeData struct {
	StartLine          int
	StartCharacter     int
	EndLine            int
	EndCharacter       int
	DefinitionResultID string
	ReferenceResultID  string
	HoverResultID      string
	MonikerIDs         idSet
}

func unmarshalRangeData(element Element) (RangeData, error) {
	type Position struct {
		Line      int `json:"line"`
		Character int `json:"character"`
	}

	type RangeVertex struct {
		Start Position `json:"start"`
		End   Position `json:"end"`
	}

	var payload RangeVertex
	if err := json.Unmarshal(element.Raw, &payload); err != nil {
		return RangeData{}, err
	}

	return RangeData{
		StartLine:      payload.Start.Line,
		StartCharacter: payload.Start.Character,
		EndLine:        payload.End.Line,
		EndCharacter:   payload.End.Character,
		MonikerIDs:     newIDSet(),
	}, nil
}

func (d RangeData) setDefinitionResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: id,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setReferenceResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  id,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setHoverResultID(id string) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      id,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d RangeData) setMonikerIDs(ids idSet) RangeData {
	return RangeData{
		StartLine:          d.StartLine,
		StartCharacter:     d.StartCharacter,
		EndLine:            d.EndLine,
		EndCharacter:       d.EndCharacter,
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         ids,
	}
}

//
//

type HoverData struct {
	// TODO - wrong?
	Result string `json:"result"`
}

func unmarshalHoverData(element Element) (payload HoverData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	// TODO
	// normalizeHover(payload.Result)
	return
}

//
//

type MonikerData struct {
	Kind                 string `json:"kind"`
	Scheme               string `json:"scheme"`
	Identifier           string `json:"identifier"`
	PackageInformationID string
}

func unmarshalMonikerData(element Element) (payload MonikerData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	if payload.Kind == "" {
		payload.Kind = "local"
	}
	return
}

func (d MonikerData) setPackageInformationID(id string) MonikerData {
	return MonikerData{
		Kind:                 d.Kind,
		Scheme:               d.Scheme,
		Identifier:           d.Identifier,
		PackageInformationID: id,
	}
}

//
//

type PackageInformationData struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func unmarshalPackageInformationData(element Element) (payload PackageInformationData, err error) {
	err = json.Unmarshal(element.Raw, &payload)
	return
}

//
//

type ResultSetData struct {
	DefinitionResultID string
	ReferenceResultID  string
	HoverResultID      string
	MonikerIDs         idSet
}

func unmarshalResultSetData(element Element) (ResultSetData, error) {
	return ResultSetData{MonikerIDs: newIDSet()}, nil
}

func (d ResultSetData) setDefinitionResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: id,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setReferenceResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  id,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setHoverResultID(id string) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      id,
		MonikerIDs:         d.MonikerIDs,
	}
}

func (d ResultSetData) setMonikerIDs(ids idSet) ResultSetData {
	return ResultSetData{
		DefinitionResultID: d.DefinitionResultID,
		ReferenceResultID:  d.ReferenceResultID,
		HoverResultID:      d.HoverResultID,
		MonikerIDs:         ids,
	}
}

//
//

type DefinitionResultData map[string]idSet

func unmarshalDefinitionResultData(element Element) (DefinitionResultData, error) {
	return map[string]idSet{}, nil
}

type ReferenceResultData map[string]idSet

func unmarshalReferenceResultData(element Element) (ReferenceResultData, error) {
	return map[string]idSet{}, nil
}
