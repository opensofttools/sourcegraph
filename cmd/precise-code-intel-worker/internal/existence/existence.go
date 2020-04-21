package existence

import (
	"path/filepath"
)

type ExistenceChecker struct {
	root        string
	dirContents map[string][]string
}

func NewExistenceChecker(repositoryID int, commit, root string, paths []string) (*ExistenceChecker, error) {
	dirContents, err := getDirectoryContents(repositoryID, commit, root, paths)
	if err != nil {
		return nil, err
	}

	return &ExistenceChecker{root, dirContents}, nil
}

func (ec *ExistenceChecker) ShouldInclude(path string) bool {
	relative := filepath.Join(ec.root, path)
	if children, ok := ec.dirContents[dirnameWithoutDot(relative)]; !ok || !includes(children, path) {
		return false
	}

	return true
}

type xbatch struct {
	parent   string
	children []Node
}

func getDirectoryContents(repositoryID int, commit, root string, paths []string) (map[string][]string, error) {
	batch := []xbatch{
		{"", makeTree(root, paths).children},
	}

	contents := map[string][]string{}
	for len(batch) > 0 {
		names := []string{}
		for _, x := range batch {
			for _, c := range x.children {
				names = append(names, filepath.Join(x.parent, c.dirname))
			}
		}

		children, err := getDirectoryChildren(repositoryID, commit, names)
		if err != nil {
			return nil, err
		}

		for k, v := range children {
			contents[k] = v
		}

		var newBatch []xbatch
		for _, x := range batch {
			if children, ok := children[x.parent]; !ok || len(children) == 0 {
				continue
			}

			for _, c := range x.children {
				newBatch = append(newBatch, xbatch{filepath.Join(x.parent, c.dirname), c.children})
			}
		}
		batch = newBatch
	}

	return contents, nil
}

// TODO - make a set instea
func includes(l []string, p string) bool {
	for _, x := range l {
		if x == p {
			return true
		}
	}

	return false
}
