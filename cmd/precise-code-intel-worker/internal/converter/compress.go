package converter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
)

func gzipJSON(x interface{}) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	err := json.NewEncoder(w).Encode(x)
	if err != nil {
		return nil, err
	}
	_ = w.Close()

	return buf.Bytes(), nil
}
