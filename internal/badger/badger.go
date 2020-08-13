package badger

import (
	"path/filepath"

	"github.com/dgraph-io/badger/v2"
)

func Open(instancePath string) (*badger.DB, error) {
	openOpts := badger.DefaultOptions(instancePath)
	openOpts.Logger = badgerLogger{}
	// Open the Badger database located in the instancePath directory.
	// It will be created if it doesn't exist.
	return badger.Open(openOpts)
}

func InstanceDir(dataDir, instanceId string) string {
	return filepath.Join(dataDir, instanceId)
}
