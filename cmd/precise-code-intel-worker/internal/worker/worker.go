package worker

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/bundles"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/converter"
	"github.com/sourcegraph/sourcegraph/cmd/precise-code-intel-worker/internal/db"
)

type Worker struct {
	db                  db.DB
	bundleManagerClient bundles.BundleManagerClient
}

func New(db db.DB, bundleManagerClient bundles.BundleManagerClient) *Worker {
	return &Worker{
		db:                  db,
		bundleManagerClient: bundleManagerClient,
	}
}

func (w *Worker) Start() error {
	for {
		upload, closer, ok, err := w.db.Dequeue(context.Background())
		if err != nil {
			return err
		}

		if !ok {
			// TODO - backoff instead
			time.Sleep(time.Second)
			continue
		}

		if err := w.process(upload, closer); err != nil {
			return err
		}
	}
}

func (w *Worker) process(upload db.Upload, closer db.TxCloser) (err error) {
	defer func() {
		// TODO - mark complete or error
		err = closer.CloseTx(err)
	}()

	name, err := ioutil.TempDir("", "")
	if err != nil {
		return err
	}
	defer func() {
		// TODO - catch error
		_ = os.RemoveAll(name)
	}()

	filename, err := w.bundleManagerClient.GetUpload(context.Background(), upload.ID, name)
	if err != nil {
		return err
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	newFilename := filepath.Join(name, uuid.String())

	packages, refs, err := converter.Convert(upload.RepositoryID, upload.Commit, upload.Root, filename, newFilename)
	if err != nil {
		return err
	}

	fmt.Printf("> %#v\n> %#v\n", packages, refs)

	// TODO - TW
	// TODO - unify types here
	// if err := w.db.UpdatePackagesAndRefs(context.Background(), nil, upload.ID, packages, refs); err != nil {
	// 	return err
	// }

	f, err := os.Open(newFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := w.bundleManagerClient.SendDB(context.Background(), upload.ID, f); err != nil {
		return err
	}

	// TODO - delete overwritten dumps
	// TODO

	// TODO - update commits and dumps visible from tip
	// TODO

	return nil
}
