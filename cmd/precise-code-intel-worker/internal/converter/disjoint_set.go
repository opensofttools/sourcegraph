package converter

type DisjointSet map[string]IDSet

func (set DisjointSet) Union(id1, id2 string) {
	set.getOrCreateSet(id1).Add(id2)
	set.getOrCreateSet(id2).Add(id1)
}

func (set DisjointSet) getOrCreateSet(id string) IDSet {
	s, ok := set[id]
	if !ok {
		s = newIDSet()
		set[id] = s
	}

	return s
}

func (set DisjointSet) ExtractSet(id string) IDSet {
	s := newIDSetFrom([]string{id})
	frontier := []string{id}
	for len(frontier) > 0 {
		var v string
		v, frontier = frontier[0], frontier[1:]
		if !s.Has(v) {
			s.Add(v)
			frontier = append(frontier, set[v].Keys()...)
		}
	}

	return s
}
