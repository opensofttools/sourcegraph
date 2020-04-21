package converter

type IDSet map[string]struct{}

func newIDSet() IDSet {
	return map[string]struct{}{}
}

func newIDSetFrom(ids []string) IDSet {
	set := newIDSet()
	for _, id := range ids {
		set.Add(id)
	}

	return set
}

func (set IDSet) Add(id string) {
	set[id] = struct{}{}
}

func (set IDSet) Has(id string) bool {
	_, ok := set[id]
	return ok
}

func (set IDSet) Keys() []string {
	var keys []string
	for k := range set {
		keys = append(keys, k)
	}

	return keys
}

// func (set IDSet) Any() (string, bool) {
// 	for k := range set {
// 		return k, true
// 	}
// 	return "", false
// }
