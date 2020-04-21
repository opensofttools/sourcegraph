package converter

import "encoding/json"

type Element interface {
	ID() string
	Type() string
	Label() string
	Raw() json.RawMessage
}

type stupidElement struct {
	_id    string
	_type  string
	_label string
	_raw   json.RawMessage
}

func (e *stupidElement) ID() string           { return e._id }
func (e *stupidElement) Type() string         { return e._type }
func (e *stupidElement) Label() string        { return e._label }
func (e *stupidElement) Raw() json.RawMessage { return e._raw }

func UnmarshalElement(raw []byte) (Element, error) {
	var payload struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Label string `json:"label"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	return &stupidElement{
		_id:    payload.ID,
		_type:  payload.Type,
		_label: payload.Label,
		_raw:   json.RawMessage(raw),
	}, nil
}

type ResultSetData struct {
	DefinitionResultID string
	ReferenceResultID  string
	HoverResultID      string
	MonikerIDs         IDSet
}

func (d ResultSetData) setDefinitionResultID(id string) ResultSetData {
	return ResultSetData{DefinitionResultID: id, ReferenceResultID: d.ReferenceResultID, HoverResultID: d.HoverResultID, MonikerIDs: d.MonikerIDs}
}

func (d ResultSetData) setReferenceResultID(id string) ResultSetData {
	return ResultSetData{DefinitionResultID: d.DefinitionResultID, ReferenceResultID: id, HoverResultID: d.HoverResultID, MonikerIDs: d.MonikerIDs}
}

func (d ResultSetData) setHoverResultID(id string) ResultSetData {
	return ResultSetData{DefinitionResultID: d.DefinitionResultID, ReferenceResultID: d.ReferenceResultID, HoverResultID: id, MonikerIDs: d.MonikerIDs}
}

func (d ResultSetData) setMonikerIDs(ids IDSet) ResultSetData {
	return ResultSetData{DefinitionResultID: d.DefinitionResultID, ReferenceResultID: d.ReferenceResultID, HoverResultID: d.HoverResultID, MonikerIDs: ids}
}

type RangeData struct {
	StartLine          int
	StartCharacter     int
	EndLine            int
	EndCharacter       int
	DefinitionResultID string
	ReferenceResultID  string
	HoverResultID      string
	MonikerIDs         IDSet
}

func (d RangeData) setDefinitionResultID(id string) RangeData {
	return RangeData{StartLine: d.StartLine, StartCharacter: d.StartCharacter, EndLine: d.EndLine, EndCharacter: d.EndCharacter, DefinitionResultID: id, ReferenceResultID: d.ReferenceResultID, HoverResultID: d.HoverResultID, MonikerIDs: d.MonikerIDs}
}

func (d RangeData) setReferenceResultID(id string) RangeData {
	return RangeData{StartLine: d.StartLine, StartCharacter: d.StartCharacter, EndLine: d.EndLine, EndCharacter: d.EndCharacter, DefinitionResultID: d.DefinitionResultID, ReferenceResultID: id, HoverResultID: d.HoverResultID, MonikerIDs: d.MonikerIDs}
}

func (d RangeData) setHoverResultID(id string) RangeData {
	return RangeData{StartLine: d.StartLine, StartCharacter: d.StartCharacter, EndLine: d.EndLine, EndCharacter: d.EndCharacter, DefinitionResultID: d.DefinitionResultID, ReferenceResultID: d.ReferenceResultID, HoverResultID: id, MonikerIDs: d.MonikerIDs}
}

func (d RangeData) setMonikerIDs(ids IDSet) RangeData {
	return RangeData{StartLine: d.StartLine, StartCharacter: d.StartCharacter, EndLine: d.EndLine, EndCharacter: d.EndCharacter, DefinitionResultID: d.DefinitionResultID, ReferenceResultID: d.ReferenceResultID, HoverResultID: d.HoverResultID, MonikerIDs: ids}
}

type MonikerData struct {
	Kind                 string
	Scheme               string
	Identifier           string
	PackageInformationID string
}

func (d MonikerData) setPackageInformationID(id string) MonikerData {
	return MonikerData{Kind: d.Kind, Scheme: d.Scheme, Identifier: d.Identifier, PackageInformationID: id}
}

type PackageInformationData struct {
	Name    string
	Version string
}

type Edge struct {
	outV string
	inVs []string

	// Optional fields
	document string
}

func unmarshalEdge(element Element) (Edge, error) {
	var payload struct {
		OutV     string   `json:"outV"` // TODO - string or int ids :(
		InV      string   `json:"inV"`
		InVs     []string `json:"inVs"`
		Document string   `json:"document"`
	}
	if err := json.Unmarshal(element.Raw(), &payload); err != nil {
		return Edge{}, nil
	}

	return Edge{
		outV:     payload.OutV,
		inVs:     append([]string{payload.InV}, payload.InVs...),
		document: payload.Document,
	}, nil
}
