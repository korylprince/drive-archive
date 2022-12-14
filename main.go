package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/korylprince/drive-archive/drive"
)

func run(auth, user, root, out string, downloadOrphans bool) error {
	svc, err := drive.NewService(auth, user, time.Second, 8)
	if err != nil {
		return fmt.Errorf("could not create service: %w", err)
	}

	if root == "" {
		root, err = svc.Root()
		if err != nil {
			return fmt.Errorf("could not get root id: %w", err)
		}
	}

	files, err := svc.List()
	if err != nil {
		return fmt.Errorf("could not list files: %w", err)
	}

	fmt.Println("found", len(files), "total files")

	rootTree, orphans := drive.NewTree(root, files)

	if err = svc.DownloadTree(rootTree, out, 0); err != nil {
		return fmt.Errorf("could not finish downloading \"My Drive\" files: %w", err)
	}

	if downloadOrphans {
		if err = svc.DownloadTree(orphans, out, 0); err != nil {
			return fmt.Errorf("could not finish downloading Shared files: %w", err)
		}
	}

	fmt.Println("done!")

	return nil
}

func main() {
	flAuthJSON := flag.String("authfile", "", "path to service account json file")
	flUser := flag.String("user", "", "email of user to download Google Drive files for")
	flRoot := flag.String("root", "", "the id of the folder to download. Leave empty to download entire Drive")
	flOrphans := flag.Bool("orphans", false, "download orphaned files. These are usually Shared Files")
	flOut := flag.String("out", "", "path to output files to. Will be created if it doesn't already exist")
	flHelp := flag.Bool("help", false, "display this help information")

	flag.Parse()

	if *flHelp {
		flag.Usage()
		os.Exit(0)
	}

	if *flAuthJSON == "" {
		flag.Usage()
		fmt.Println("\n-authfile must be set")
		os.Exit(-1)
	}

	if *flUser == "" {
		flag.Usage()
		fmt.Println("\n-user must be set")
		os.Exit(-1)
	}

	if *flOut == "" {
		flag.Usage()
		fmt.Println("\n-out must be set")
		os.Exit(-1)
	}

	if *flRoot != "" && *flOrphans {
		flag.Usage()
		fmt.Println("\n-orphans cannot be used when -root is set")
		os.Exit(-1)
	}

	if err := os.MkdirAll(*flOut, 0755); err != nil {
		fmt.Println("could not create output directory:", err)
		os.Exit(-1)
	}

	err := run(*flAuthJSON, *flUser, *flRoot, *flOut, *flOrphans)
	if err != nil {
		fmt.Println("could not download files:", err)
		os.Exit(-1)
	}
}
