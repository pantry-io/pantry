package reaper

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/gofrs/uuid"
	badgerInternal "github.com/nickpoorman/nats-requeue/internal/badger"
	"github.com/stretchr/testify/assert"
)

var (
	uniqueId int64
)

func getUniqueId() int64 {
	return atomic.AddInt64(&uniqueId, 1)
}

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

func createBadgerInstance(dataDir string) (string, *badger.DB, error) {
	instanceId := uuid.Must(uuid.NewV4()).String()
	path := badgerInternal.InstanceDir(dataDir, instanceId)

	// Create a new badger instance
	db, err := badgerInternal.Open(path)
	if err != nil {
		return instanceId, nil, err
	}
	return instanceId, db, nil
}

func createZombieInstance(dataDir string) (string, []kv, error) {
	var kvs []kv
	instanceId, db, err := createBadgerInstance(dataDir)
	if err != nil {
		return instanceId, kvs, err
	}
	defer db.Close()

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	for i := 0; i < 1000000; i++ {
		key := []byte(fmt.Sprintf("key.%d.%d", i, getUniqueId()))
		value := []byte(fmt.Sprintf("value.%d.%d", i, getUniqueId()))
		if err := wb.Set(key, value); err != nil {
			return instanceId, kvs, err
		}
		kvs = append(kvs, kv{k: key, v: value})
	}
	err = wb.Flush() // Wait for all txns to finish.

	return instanceId, kvs, err
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

func fileExists(path string) bool {
	if f, err := os.Stat(path); err == nil {
		fmt.Printf("found file: %s | size: %d", path, f.Size())
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		panic(fmt.Errorf("file existence is unknown: %w", err))
	}
}

func TestReaper_AlreadyOpen(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our dst instance
	dstInstanceId, dstDb, err := createBadgerInstance(dataDir)
	assert.NoError(t, err, "should have been able to create badger instance")
	defer dstDb.Close()
	dstInstancePath := badgerInternal.InstanceDir(dataDir, dstInstanceId)

	// Create our zombie instance
	zombieInstanceId, _, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	zombiePath := badgerInternal.InstanceDir(dataDir, zombieInstanceId)

	// Hold open the instance
	db, err := badgerInternal.Open(zombiePath)
	if err != nil {
		t.Fatal(fmt.Errorf("problem holding open the instance: %w", err))
	}
	defer db.Close()

	// Create our reaper
	reaper, err := NewReaper(
		nil,
		dataDir,
		dstInstancePath,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	ok, err := reaper.mergeInstance(zombiePath)
	assert.NoError(t, err, "should not return an error when trying to merge a locked instance")
	assert.False(t, ok, "should not have been able to merge the instance")
}

func TestReaper_getOtherInstanceIds(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our dst instance
	dstInstanceId, dstDb, err := createBadgerInstance(dataDir)
	assert.NoError(t, err, "should have been able to create badger instance")
	defer dstDb.Close()
	dstInstancePath := badgerInternal.InstanceDir(dataDir, dstInstanceId)

	// Create our zombie instance
	zombieInstanceId, _, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}

	reaper, err := NewReaper(
		dstDb,
		dataDir,
		dstInstancePath,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	paths, err := reaper.getOtherInstanceIds()
	assert.NoError(t, err, "should not have received an error")
	assert.Len(t, paths, 1, "paths returned should only be of length 1")
	assert.Equal(
		t,
		[]string{zombieInstanceId}[:],
		paths,
		"should contain only the zombie instance id",
	)
}

func TestReaper_Reap(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our dst instance
	dstInstanceId, dstDb, err := createBadgerInstance(dataDir)
	assert.NoError(t, err, "should have been able to create badger instance")
	defer dstDb.Close()
	dstInstancePath := badgerInternal.InstanceDir(dataDir, dstInstanceId)

	// Create our zombie instance
	zombieInstanceId, kvs, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	zombiePath := badgerInternal.InstanceDir(dataDir, zombieInstanceId)

	// Create our reaper
	reaper, err := NewReaper(
		dstDb,
		dataDir,
		dstInstancePath,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	// Trigger a reap
	assert.NoError(t, reaper.reap(), "no error should have been returned from reap")

	// Verify the merged data is now in our new instance.
	if err := verifyData(dstDb, kvs); err != nil {
		t.Fatal(fmt.Errorf("problem verifying data in dst: %w", err))
	}

	// Verify that the src instance directory (zombie) has been removed.
	assert.False(t, fileExists(zombiePath), "file should not exist: %s", zombiePath)
}

func TestReaper_AlreadyRemovedInstanceDirectoryRace(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our dst instance
	dstInstanceId, dstDb, err := createBadgerInstance(dataDir)
	assert.NoError(t, err, "should have been able to create badger instance")
	defer dstDb.Close()
	dstInstancePath := badgerInternal.InstanceDir(dataDir, dstInstanceId)

	// Create our zombie instance
	zombieInstanceId, _, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	zombiePath := badgerInternal.InstanceDir(dataDir, zombieInstanceId)

	// Create our reaper
	reaper, err := NewReaper(
		dstDb,
		dataDir,
		dstInstancePath,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	// Trigger a mergeInstance
	merged, err := reaper.mergeInstance(zombiePath)
	assert.True(t, merged, "should have merged")
	assert.NoError(t, err, "error should be nil")

	// Remove the zombie directory
	assert.NoError(t, reaper.removeInstance(zombiePath), "error should be nil")

	// Verify that the src instance directory (zombie) has been removed.
	assert.False(t, fileExists(zombiePath), "zombie instance should not exist: %s", zombiePath)
}

func TestReaper_RemoveAlreadyOpenInstanceDirectoryRace(t *testing.T) {
	// Create a directory just for this test.
	dataDir := setup(t)

	// Create our dst instance
	dstInstanceId, dstDb, err := createBadgerInstance(dataDir)
	assert.NoError(t, err, "should have been able to create badger instance")
	defer dstDb.Close()
	dstInstancePath := badgerInternal.InstanceDir(dataDir, dstInstanceId)

	// Create our zombie instance
	zombieInstanceId, _, err := createZombieInstance(dataDir)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating zombie instance: %w", err))
	}
	zombiePath := badgerInternal.InstanceDir(dataDir, zombieInstanceId)

	// Create our reaper
	reaper, err := NewReaper(
		dstDb,
		dataDir,
		dstInstancePath,
		ReapInterval(10000*time.Hour),
	)
	if err != nil {
		t.Fatal(fmt.Errorf("problem creating reaper: %w", err))
	}

	// Trigger a mergeInstance
	merged, err := reaper.mergeInstance(zombiePath)
	assert.True(t, merged, "should have merged")
	assert.NoError(t, err, "error should be nil")

	// Open the zombie directory
	db, err := badgerInternal.Open(zombiePath)
	assert.NoError(t, err, "should be no error opening the zombie instance")
	defer db.Close()

	// Attempt to remove the zombie instance
	assert.Error(t, reaper.removeInstance(zombiePath),
		"should error when trying to remove a zombie instance that's locked")

	// Verify that the src instance directory (zombie) has not been removed.
	assert.True(t, fileExists(zombiePath), "zombie instance should exist: %s", zombiePath)

	assert.NoError(t, db.Close(), "should not be an error when closing")
}
