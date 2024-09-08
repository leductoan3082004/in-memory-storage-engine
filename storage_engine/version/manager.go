package version

import (
	"context"
	"in-memory-storage-engine/storage_engine/appCommon"
	"sync"
)

type VersionManager interface {
	AddNewVersion(version *valueVersion)
	Set(ctx context.Context, value interface{}, txID int)
	Delete(ctx context.Context, txID int) error
	GetCommitted(ctx context.Context) interface{}
	GetValueForTransaction(ctx context.Context, txID int) interface{}
}

type versionManager struct {
	rwLock   *sync.RWMutex
	versions valueVersions // contain only committed versions
}

func NewValueVersionManager() VersionManager {
	return &versionManager{
		versions: valueVersions{},
		rwLock:   new(sync.RWMutex),
	}
}

func (manager *versionManager) AddNewVersion(version *valueVersion) {
	manager.versions = append(manager.versions, version)
}

func (manager *versionManager) Set(ctx context.Context, value interface{}, txID int) {
	manager.rwLock.Lock()
	defer manager.rwLock.Unlock()
	manager.AddNewVersion(newSetValueVersion(value, txID))
}

func (manager *versionManager) getCommitedInternal(ctx context.Context) interface{} {
	if len(manager.versions) == 0 {
		return nil
	}

	return manager.versions[len(manager.versions)-1].value
}

func (manager *versionManager) Delete(ctx context.Context, txID int) error {
	manager.rwLock.Lock()
	defer manager.rwLock.Unlock()

	if manager.getCommitedInternal(ctx) != nil {
		manager.AddNewVersion(newDeleteValueVersion(txID))
	} else {
		return appCommon.KeyDoesNotExist
	}
	return nil
}

func (manager *versionManager) GetCommitted(ctx context.Context) interface{} {
	manager.rwLock.RLock()
	defer manager.rwLock.RUnlock()

	return manager.getCommitedInternal(ctx)
}

func (manager *versionManager) GetValueForTransaction(ctx context.Context, txID int) interface{} {
	manager.rwLock.RLock()
	defer manager.rwLock.RUnlock()
	for i := len(manager.versions) - 1; i >= 0; i-- {
		if manager.versions[i].txID <= txID {
			if manager.versions[i].isVisible {
				return manager.versions[i].value
			} else {
				return nil
			}
		}
	}
	return nil
}
