package drive

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const FileTypeFolder = "application/vnd.google-apps.folder"
const FileTypeShortcut = "application/vnd.google-apps.shortcut"
const FileTypeSDKPrefix = "application/vnd.google-apps.drive-sdk."

const ErrReasonSizeLimitExceeded = "exportSizeLimitExceeded"
const ErrReasonRateLimitExceeded = "rateLimitExceeded"

var ErrNoExportableFormat = errors.New("no exportable format")

var ExportTypes = map[string]string{
	"application/vnd.google-apps.document":     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
	"application/vnd.google-apps.presentation": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
	"application/vnd.google-apps.spreadsheet":  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	"application/vnd.google-apps.drawing":      "image/svg+xml",
	"application/vnd.google-apps.jam":          "application/pdf",
	"application/vnd.google-apps.script":       "application/vnd.google-apps.script+json",
	"application/vnd.google-apps.form":         "application/zip",
	"application/vnd.google-apps.site":         "text/plain",
}

var ExportExtensions = map[string]string{
	"application/vnd.google-apps.document":     ".docx",
	"application/vnd.google-apps.presentation": ".pptx",
	"application/vnd.google-apps.spreadsheet":  ".xlsx",
	"application/vnd.google-apps.drawing":      ".svg",
	"application/vnd.google-apps.jam":          ".pdf",
	"application/vnd.google-apps.script":       ".json",
	"application/vnd.google-apps.form":         ".zip",
	"application/vnd.google-apps.site":         ".txt",
}

var SkipTypes = map[string]struct{}{
	"application/vnd.google-apps.fusiontable": {},
	"application/vnd.google-apps.map":         {},
}

// checkRetry returns true if a retry should be tried
func checkRetry(err error) bool {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		switch gErr.Code {
		case 400, 401, 404, 501:
			return false
		case 403:
			for _, e := range gErr.Errors {
				if e.Reason == ErrReasonRateLimitExceeded {
					return true
				}
			}
			return false
		}
		return true
	}
	return false
}

// retry retries f() with exponential backoff
func retry(start time.Duration, maxTries int, f func() error) error {
	tries := 0
	for {
		err := f()
		if err == nil {
			return nil
		}

		tries += 1
		if tries == maxTries {
			return err
		}

		if !checkRetry(err) {
			return err
		}

		time.Sleep(start)
		start *= 2
	}
}

// Service is a Google Drive file service
type Service struct {
	*drive.FilesService
	initialBackoff time.Duration
	tries          int
	client         *http.Client
}

// NewService returns a new service using the service account credentials JSON file found at configPath for the given user
// initialBackoff and tries are used to configure an exponential backoff strategy. Set tries to 1 to disable retries or set tries to <= 0 to retry infinitely
//
// To create the JSON file for configPath:
//
//  * Create or open a project at https://console.cloud.google.com
//  * Create a new service account at IAM & Admin -> Service Accounts
//  * Add a new key to the service account (as JSON)
//  * Add the client_id found in the JSON file to [Domain-wide Delegation](https://admin.google.com/ac/owl/domainwidedelegation)
//    * Add the https://www.googleapis.com/auth/drive and https://www.googleapis.com/auth/drive.metadata scopes
func NewService(configPath, user string, initialBackoff time.Duration, tries int) (*Service, error) {
	buf, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("could not read config: %w", err)
	}

	config, err := google.JWTConfigFromJSON(buf, drive.DriveScope, drive.DriveMetadataScope)
	if err != nil {
		return nil, fmt.Errorf("could not parse config: %w", err)
	}
	config.Subject = user

	client := config.Client(context.Background())

	driveSvc, err := drive.NewService(context.Background(), option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("Could not create drive service: %w", err)
	}

	return &Service{FilesService: drive.NewFilesService(driveSvc), initialBackoff: initialBackoff, tries: tries, client: client}, nil
}

// Root returns the root folder ID of the user's Google Drive
func (s *Service) Root() (string, error) {
	var id string
	if err := retry(s.initialBackoff, s.tries, func() error {
		file, err := s.FilesService.Get("root").Fields("id").Do()
		if err != nil {
			return fmt.Errorf("could not get root: %w", err)
		}
		id = file.Id
		return nil
	}); err != nil {
		return "", err
	}
	return id, nil
}

// List returns all files in the user's Google Drive
func (s *Service) List() ([]*drive.File, error) {
	var files []*drive.File
	cmd := s.FilesService.List().
		Corpora("user").
		Fields(
			"nextPageToken",
			"files/id",
			"files/name",
			"files/mimeType",
			"files/md5Checksum",
			"files/modifiedTime",
			"files/parents",
			"files/shortcutDetails/targetId",
			"files/exportLinks",
		).
		Spaces("drive").
		PageSize(1000)

	var (
		resp *drive.FileList
		err  error
	)
	for {
		if err = retry(s.initialBackoff, s.tries, func() error {
			resp, err = cmd.Do()
			if err != nil {
				return fmt.Errorf("could not list files: %w", err)
			}
			return nil
		}); err != nil {
			return nil, err
		}
		files = append(files, resp.Files...)
		if resp.NextPageToken == "" {
			return files, nil
		}
		cmd.PageToken(resp.NextPageToken)
	}
}

// get returns the *drive.File corresponding to id
// func (s *Service) get(id string) (*drive.File, error) {
// 	cmd := s.FilesService.Get(id).
// 		Fields(
// 			"id",
// 			"mimeType",
// 			"md5Checksum",
// 			"modifiedTime",
// 			"exportLinks",
// 		)

// 	var (
// 		resp *drive.File
// 		err  error
// 	)
// 	if err = retry(s.initialBackoff, s.tries, func() error {
// 		resp, err = cmd.Do()
// 		if err != nil {
// 			return fmt.Errorf("could not get file: %w", err)
// 		}
// 		return nil
// 	}); err != nil {
// 		return nil, err
// 	}

// 	return resp, nil
// }

func writeBody(r io.Reader, path, timestamp string) error {
	// write file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("could not create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("could not write export body: %w", err)
	}

	// set mtime
	if timestamp == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return fmt.Errorf("could not parse modified time: %w", err)
	}

	if err = os.Chtimes(path, t, t); err != nil {
		return fmt.Errorf("could not change mtime: %w", err)
	}

	return nil
}

func (s *Service) exportAlt(file *drive.File, mimeType, path string) error {
	var url string
	for mime, u := range file.ExportLinks {
		if mime == mimeType {
			url = u
			break
		}
	}
	if url == "" {
		return errors.New("could not complete export request: no export link found")
	}

	var (
		resp *http.Response
		err  error
	)

	if err = retry(s.initialBackoff, s.tries, func() error {
		resp, err = s.client.Get(url)
		if err != nil {
			return fmt.Errorf("could not complete export link request: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	defer resp.Body.Close()

	return writeBody(resp.Body, path, file.ModifiedTime)
}

// Export exports (with specified mime type) the file with id to path.
// Most users should use DownloadFile instead
func (s *Service) Export(file *drive.File, mimeType, path string) error {
	var (
		resp *http.Response
		err  error
	)
	if err = retry(s.initialBackoff, s.tries, func() error {
		resp, err = s.FilesService.Export(file.Id, mimeType).Download()
		if err != nil {
			return fmt.Errorf("could not complete export request: %w", err)
		}
		return nil
	}); err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) {
			for _, e := range gErr.Errors {
				if e.Reason == ErrReasonSizeLimitExceeded {
					return s.exportAlt(file, mimeType, path)
				}
			}
		}
		return err
	}
	defer resp.Body.Close()

	return writeBody(resp.Body, path, file.ModifiedTime)
}

// Download downloads the file with id to path.
// Most users should use DownloadFile instead
func (s *Service) Download(file *drive.File, path string) error {
	var (
		resp *http.Response
		err  error
	)
	if err = retry(s.initialBackoff, s.tries, func() error {
		resp, err = s.Get(file.Id).Download()
		if err != nil {
			return fmt.Errorf("could not complete download request: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	defer resp.Body.Close()

	return writeBody(resp.Body, path, file.ModifiedTime)
}

// md5Verify returns true if a file exists at path and md5(file) == hash
func md5Verify(path, hash string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	h := md5.New()
	_, err = io.Copy(h, f)
	if err != nil {
		return false
	}

	return hex.EncodeToString(h.Sum(nil)[:]) == hash
}

// mtimeVerify returns true if a file exists at path and mtime(file) >= t
func mtimeVerify(path string, t time.Time) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}

	return !info.ModTime().Before(t)
}

// DownloadFile downloads f to path. It automatically resolves shortcuts and converts Google Docs, Slides, Sheets, and Drawings to downloadable formats.
// If downloaded is false, the file was not downloaded because the existing file matched.
func (s *Service) DownloadFile(f *drive.File, path string) (downloaded bool, err error) {
	// check for skipped mime types
	if _, ok := SkipTypes[f.MimeType]; ok || strings.HasPrefix(f.MimeType, FileTypeSDKPrefix) {
		return false, ErrNoExportableFormat
	}

	// if google docs file, download exported file
	if typ, ok := ExportTypes[f.MimeType]; ok {
		// don't download exported file if mtime is same
		if f.ModifiedTime != "" {
			t, err := time.Parse(time.RFC3339, f.ModifiedTime)
			if err == nil && mtimeVerify(path, t) {
				return false, nil
			}
		}

		return true, s.Export(f, typ, path)
	}

	// don't download file if md5sum is same
	if md5Verify(path, f.Md5Checksum) {
		return false, nil
	}

	// otherwise, download file directly
	return true, s.Download(f, path)
}
