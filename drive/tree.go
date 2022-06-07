package drive

import (
	"path/filepath"
	"regexp"
	"sort"

	"google.golang.org/api/drive/v3"
)

// ValidPathChars is the set of valid path name characters
var ValidPathChars = regexp.MustCompile("[^a-zA-Z0-9 !@#$%^&()\\-_=+\\[\\]{}';\\.,`~]")

// File represents a Google Drive File or Folder. A Google Drive object can have multiple parents
type File struct {
	ID             string
	Name           string
	File           *drive.File
	Files          []*File
	Parents        []*File
	ShortcutTarget *File
}

// IsFolder returns true if the File is a folder
func (fi *File) IsFolder() bool {
	return fi.File.MimeType == FileTypeFolder
}

// NewTree parses a list of Google Drive files and returns two trees: a tree rooted at the user's Google Drive (specified by rootID) and an "Other Files" tree which includes all files not under the main tree.
func NewTree(rootID string, list []*drive.File) (tree, orphaned *File) {
	// create root
	root := &File{ID: rootID, Name: "My Drive", File: &drive.File{MimeType: FileTypeFolder}, Files: make([]*File, 0)}

	// first pass: create nodes
	nodes := map[string]*File{rootID: root}
	for _, f := range list {
		// skip root node if it's in file list
		if f.Id == root.ID {
			continue
		}
		file := &File{ID: f.Id, Name: f.Name, File: f, Parents: make([]*File, 0, 1)}
		if f.MimeType == FileTypeFolder {
			file.Files = make([]*File, 0)
		}
		nodes[f.Id] = file
	}

	// second pass: resolve shortcuts
	for _, f := range nodes {
		if f.File.MimeType == FileTypeShortcut {
			if f.File.ShortcutDetails != nil {
				if tgt, ok := nodes[f.File.ShortcutDetails.TargetId]; ok {
					f.ShortcutTarget = tgt
				}
			}
		}
	}

	// create orphan tree
	orphans := &File{Name: "Other Files", File: &drive.File{MimeType: FileTypeFolder}, Files: make([]*File, 0)}

	// third pass: connect nodes
	for _, f := range nodes {
		// make sure root node stays root
		if f.ID == root.ID {
			continue
		}
		// connect parent
		found := false
		for _, pid := range f.File.Parents {
			if p, ok := nodes[pid]; ok && p.IsFolder() {
				found = true
				p.Files = append(p.Files, f)
				f.Parents = append(f.Parents, p)
			}
		}

		// connect unconnected nodes to orphan tree
		if !found {
			orphans.Files = append(orphans.Files, f)
			f.Parents = append(f.Parents, orphans)
		}
	}

	// deterministically sort the tree
	sortfunc := func(path string, file *File) error {
		if file.Files != nil {
			sort.SliceStable(file.Files, func(i, j int) bool {
				ni := ValidPathChars.ReplaceAllString(file.Files[i].Name, "")
				nj := ValidPathChars.ReplaceAllString(file.Files[j].Name, "")
				if ni == nj {
					return file.Files[i].ID < file.Files[j].ID
				}
				return ni < nj
			})
		}
		if file.Parents != nil {
			sort.SliceStable(file.Parents, func(i, j int) bool {
				ni := ValidPathChars.ReplaceAllString(file.Parents[i].Name, "")
				nj := ValidPathChars.ReplaceAllString(file.Parents[j].Name, "")
				if ni == nj {
					return file.Parents[i].ID < file.Parents[j].ID
				}
				return ni < nj
			})
		}
		return nil
	}

	root.Walk(sortfunc)
	orphans.Walk(sortfunc)

	return root, orphans
}

// Walk walks through all of the files in the tree and calls f() on them. The current file and full path to the file is passed to f(). If f() returns an error, iteration and the error is returned.
func (fi *File) Walk(f func(path string, file *File) error) error {
	type node struct {
		f       *File
		path    string
		parents map[string]struct{}
	}

	q := []*node{{f: fi, path: ValidPathChars.ReplaceAllString(fi.Name, ""), parents: make(map[string]struct{})}}
	for len(q) > 0 {
		// pop file
		n := q[0]
		q = q[1:]

		// prevent loops
		if _, ok := n.parents[n.f.ID]; ok {
			continue
		}

		// resolve shortcuts
		if n.f.ShortcutTarget != nil {
			n.f = n.f.ShortcutTarget
		}

		if err := f(n.path, n.f); err != nil {
			return err
		}

		for _, c := range n.f.Files {
			p := map[string]struct{}{n.f.ID: {}}
			for k, v := range n.parents {
				p[k] = v
			}
			q = append(q, &node{f: c, path: filepath.Join(n.path, ValidPathChars.ReplaceAllString(c.Name, "")), parents: p})
		}

	}
	return nil
}
