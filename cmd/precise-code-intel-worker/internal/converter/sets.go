package converter

type defaultIDSetMap map[string]idSet

func (sm defaultIDSetMap) getOrCreate(key string) idSet {
	if s, ok := sm[key]; ok {
		return s
	}

	s := newIDSet()
	sm[key] = s
	return s
}

type idSet map[string]struct{}

func newIDSet() idSet {
	return map[string]struct{}{}
}

func newSingletonIDSet(id string) idSet {
	set := newIDSet()
	set.add(id)
	return set
}

func (set idSet) add(id string) {
	set[id] = struct{}{}
}

func (set idSet) addAll(other idSet) {
	for k := range other {
		set.add(k)
	}
}

func (set idSet) contains(id string) bool {
	_, ok := set[id]
	return ok
}

func (set idSet) keys() []string {
	var keys []string
	for k := range set {
		keys = append(keys, k)
	}

	return keys
}

func (set idSet) choose() (string, bool) {
	for k := range set {
		return k, true
	}
	return "", false
}

type disjointIDSet map[string]idSet

func newDisjointIDSet() disjointIDSet {
	return map[string]idSet{}
}

func (set disjointIDSet) union(id1, id2 string) {
	set.getOrCreateSet(id1).add(id2)
	set.getOrCreateSet(id2).add(id1)
}

func (set disjointIDSet) extractSet(id string) idSet {
	s := newSingletonIDSet(id)

	frontier := []string{id}
	for len(frontier) > 0 {
		v := frontier[0]
		frontier = frontier[1:]

		if !s.contains(v) {
			s.add(v)
			frontier = append(frontier, set[v].keys()...)
		}
	}

	return s
}

func (set disjointIDSet) getOrCreateSet(id string) idSet {
	s, ok := set[id]
	if !ok {
		s = newIDSet()
		set[id] = s
	}

	return s
}
