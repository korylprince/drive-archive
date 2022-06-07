package drive

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sync/errgroup"
)

type download struct {
	*File
	Path string
}

func (s *Service) downloader(outpath string, c <-chan *download) error {
	for d := range c {
		downloaded, err := s.DownloadFile(d.File.File, filepath.Join(outpath, d.Path))
		if err != nil {
			fmt.Printf("%s: could not download file: %v\n", d.Path, err)
			continue
		}
		if !downloaded {
			fmt.Printf("%s: skipped existing file\n", d.Path)
			continue
		}
		fmt.Printf("%s: downloaded\n", d.Path)
	}

	return nil
}

// DownloadTree downloads the file tree rooted at root to outpath using the specified number of downloaders.
// If downloaders is less than 1, runtime.NumCPU() will be used
func (s *Service) DownloadTree(root *File, outpath string, downloaders int) error {
	eg := new(errgroup.Group)
	c := make(chan *download)
	if downloaders < 1 {
		downloaders = runtime.NumCPU()
	}
	for i := 0; i < downloaders; i++ {
		eg.Go(func() error {
			return s.downloader(outpath, c)
		})
	}

	files := make(map[string]int)

	if err := root.Walk(func(path string, f *File) error {
		if f.IsFolder() {
			if err := os.MkdirAll(filepath.Join(outpath, path), 0755); err != nil {
				return fmt.Errorf("%s: could not create directory: %w", path, err)
			}
			fmt.Printf("%s: created directory\n", path)
			return nil
		}

		if f.File.MimeType == FileTypeShortcut {
			fmt.Printf("%s: could not resolve shortcut\n", path)
			return nil
		}

		// add extensions to exported files
		if ext, ok := ExportExtensions[f.File.MimeType]; ok {
			path += ext
		}

		// make sure there are no duplicate paths.
		// If path exists, add _# to file name and check again
	checkpath:
		files[path] += 1
		if n := files[path]; n > 1 {
			ext := filepath.Ext(path)
			base := path[:len(path)-len(ext)]
			path = fmt.Sprintf("%s_%d%s", base, n, ext)
			goto checkpath
		}

		c <- &download{File: f, Path: path}

		return nil
	}); err != nil {
		close(c)
		eg.Wait()
		return fmt.Errorf("could not finish walking tree: %w", err)
	}

	close(c)
	eg.Wait()
	return nil
}
