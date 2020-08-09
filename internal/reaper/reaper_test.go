package reaper

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/gofrs/uuid"
	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/stretchr/testify/assert"
)

func cleanUp(t *testing.T, path string) {
	t.Cleanup(func() {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	})
}

func setup(t *testing.T) string {
	dir, err := ioutil.TempDir("", fmt.Sprintf("%s-*", t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	cleanUp(t, dir)
	return dir
}

func createZombieInstance(dataDir string) (string, error) {
	instanceId := uuid.Must(uuid.NewV4()).String()
	path := badgerInternal.InstanceDir(dataDir, instanceId)

	// Create a new badger instance
	db, err := badgerInternal.Open(path)
	if err != nil {
		return instanceId, err
	}
	defer db.Close()

	// Write some data to it
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("key.%d", i)
			value := fmt.Sprintf("value.%d", i)
			if err := txn.Set([]byte(key), []byte(value)); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return instanceId, err
	}

	return instanceId, err
}

func TestReaper_AlreadyOpen(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our zombie instance
	instanceId, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	path := badgerInternal.InstanceDir(dataDir, instanceId)

	// Hold open the instance
	db, err := badgerInternal.Open(path)
	if err != nil {
		t.Fatal(fmt.Errorf("problem holding open the instance: %w", err))
	}
	defer db.Close()

	// Create our reaper
	reaper, err := NewReaper(
		dataDir,
		filepath.Join(dataDir, instanceId),
		ReapInterval(1*time.Second),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	ok, err := reaper.mergeInstance(dataDir, instanceId)
	assert.NoError(t, err, "should not return an error when trying to merge a locked instance")
	assert.False(t, ok, "should not have been able to merge the instance")
}

// func TestReaper_AlreadyOpen(t *testing.T) {
// 	// Create a directory just for this test.
// 	dataDir := setup(t)

// 	// Create our zombie instance
// 	instanceId, err := createZombieInstance(dataDir)
// 	if err != nil {
// 		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
// 	}
// 	path := badgerInternal.InstanceDir(dataDir, instanceId)

// 	// Hold open the instance
// 	db, err := badgerInternal.Open(path)
// 	if err != nil {
// 		t.Fatal(fmt.Errorf("problem holding open the instance: %w", err))
// 	}
// 	defer db.Close()

// 	// Create our reaper
// 	reaper, err := NewReaper(
// 		dataDir,
// 		filepath.Join(dataDir, instanceId),
// 		ReapInterval(1*time.Second),
// 	)
// 	if err != nil {
// 		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
// 	}

// 	_, err = reaper.mergeInstance(dataDir, instanceId)
// 	if err != nil {
// 		t.Fatal(fmt.Errorf("problem merging instance: %w", err))
// 	}
// }
