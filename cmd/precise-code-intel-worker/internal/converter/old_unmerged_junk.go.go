package converter

import (
	"encoding/json"
	"strconv"
)

// hashKey hashes a string identifier into the range [0, maxIndex)`. The
// hash algorithm here is similar ot the one used in Java's String.hashCode.
// This implementation is identical to the TypeScript version used before
// the port to Go so that we can continue to read old conversions without
// a migration.
func hashKey(id string, maxIndex int) int {
	hash := int32(0)
	for _, c := range string(id) {
		hash = (hash << 5) - hash + int32(c)
	}

	if hash < 0 {
		hash = -hash
	}

	return int(hash % int32(maxIndex))
}

type ID string

// UnmarshalJSON converts a JSON number or string into an identifier. This
// maintains the same functionality that exists on the TypeScript side by
// simply running JSON.parse() on document and result chunk data blobs.
func (id *ID) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		return json.Unmarshal(b, (*string)(id))
	}

	var value int64
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}

	*id = ID(strconv.FormatInt(value, 10))
	return nil
}
