package reaper

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
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

type kv struct {
	k []byte
	v []byte
}

func createZombieInstance(dataDir string) (string, []kv, error) {
	var kvs []kv
	instanceId := uuid.Must(uuid.NewV4()).String()
	path := badgerInternal.InstanceDir(dataDir, instanceId)

	// Create a new badger instance
	db, err := badgerInternal.Open(path)
	if err != nil {
		return instanceId, kvs, err
	}
	defer db.Close()

	// Write some data to it
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key.%d", i))
			value := []byte(fmt.Sprintf("value.%d", i))
			if err := txn.Set(key, value); err != nil {
				return err
			}
			kvs = append(kvs, kv{k: key, v: value})
		}

		return nil
	}); err != nil {
		return instanceId, kvs, err
	}

	return instanceId, kvs, err
}

func TestReaper_AlreadyOpen(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our zombie instance
	instanceId, _, err := createZombieInstance(dataDir)
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
		nil,
		dataDir,
		path,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	ok, err := reaper.mergeInstance(dataDir, instanceId)
	assert.NoError(t, err, "should not return an error when trying to merge a locked instance")
	assert.False(t, ok, "should not have been able to merge the instance")
}

func TestReaper_MergeInstance(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our zombie instance
	instanceId, kvs, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	path := badgerInternal.InstanceDir(dataDir, instanceId)

	// Create a new badger instance to be the destination.
	dstInstanceId := uuid.Must(uuid.NewV4()).String()
	dstPath := badgerInternal.InstanceDir(dataDir, dstInstanceId)
	// Create a new badger instance
	dstDB, err := badgerInternal.Open(dstPath)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating dst db: %w", err))
	}

	// Create our reaper
	reaper, err := NewReaper(
		dstDB,
		dataDir,
		path,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	ok, err := reaper.mergeInstance(dataDir, instanceId)
	if err != nil {
		t.Fatal(fmt.Errorf("problem merging instance: %w", err))
	}
	assert.True(t, ok, "should have been able to merge the instance")

	// Verify the merged data is now in our new instance.
	if err := verifyData(dstDB, kvs); err != nil {
		t.Fatal(fmt.Errorf("problem verifying data in dst: %w", err))
	}

	// TODO: Verify that the src instance directory has been removed.
}

func verifyData(db *badger.DB, kvs []kv) error {
	for _, kv := range kvs {
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(kv.k)
			if err != nil {
				return fmt.Errorf("[%s]: %w", kv.k, err)
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if !bytes.Equal(kv.v, valCopy) {
				return errors.New("values are not equal")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
