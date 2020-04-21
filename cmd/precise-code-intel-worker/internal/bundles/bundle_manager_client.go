package bundles

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type BundleManagerClient interface {
	GetUpload(ctx context.Context, bundleID int, dir string) (string, error)
	SendDB(ctx context.Context, bundleID int, r io.Reader) error
}

type bundleManagerClientImpl struct {
	bundleManagerURL string
}

var _ BundleManagerClient = &bundleManagerClientImpl{}

func New(bundleManagerURL string) BundleManagerClient {
	return &bundleManagerClientImpl{bundleManagerURL: bundleManagerURL}
}

func (c *bundleManagerClientImpl) GetUpload(ctx context.Context, bundleID int, dir string) (string, error) {
	url, err := url.Parse(fmt.Sprintf("%s/uploads/%d", c.bundleManagerURL, bundleID))
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return "", err
	}

	// TODO - use context
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	filename := filepath.Join(dir, uuid.String())

	f, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return "", err
	}

	return filename, nil
}

func (c *bundleManagerClientImpl) SendDB(ctx context.Context, bundleID int, r io.Reader) error {
	url, err := url.Parse(fmt.Sprintf("%s/dbs/%d", c.bundleManagerURL, bundleID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), r)
	if err != nil {
		return err
	}

	// TODO - use context
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	// TODO - safe to not close body here?

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	return nil
}
