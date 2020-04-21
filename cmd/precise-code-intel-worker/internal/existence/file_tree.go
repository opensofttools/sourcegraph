package existence

import (
	"path/filepath"
	"strings"
)

func makeTree(root string, documentPaths []string) Node {
	dirMap := map[string]struct{}{}
	for _, path := range documentPaths {
		if dir := dirnameWithoutDot(filepath.Join(root, path)); !strings.HasPrefix(dir, "..") {
			dirMap[dir] = struct{}{}
		}
	}

	rootNode := Node{}
	for dir := range dirMap {
		rootNode = insert(rootNode, strings.Split(dir, "/"))
	}

	return rootNode
}

type Node struct {
	dirname  string
	children []Node
}

func insert(n Node, pathSegments []string) Node {
	if len(pathSegments) == 0 {
		return n
	}

	for i, c := range n.children {
		if c.dirname == pathSegments[0] {
			n.children[i] = insert(c, pathSegments[1:])
			return n
		}
	}

	n.children = append(n.children, insert(Node{dirname: pathSegments[0]}, pathSegments[1:]))
	return n
}

func dirnameWithoutDot(dir string) string {
	if dir == "." {
		return ""
	}

	// TODO - see what behavior of this is in Go
	return filepath.Dir(dir)
}
