package version

import (
	"context"
	"in-memory-storage-engine/appCommon"
	"sync"
	"time"
)

type VersionManager interface {
	Set(ctx context.Context, value interface{}, txID int)
	Delete(ctx context.Context, txID int) error
	GetCommitted(ctx context.Context) interface{}
	GetValueBeforeTransaction(ctx context.Context, txID int) interface{}
	GetLatestVersionForKey(ctx context.Context) (int, error)
	RemoveOldVersion(ctx context.Context) error
}

type versionManager struct {
	rwMutex  *sync.RWMutex
	versions valueVersions // contain only committed versions
}

func NewValueVersionManager() VersionManager {
	return &versionManager{
		versions: valueVersions{},
		rwMutex:  new(sync.RWMutex),
	}
}

func (manager *versionManager) AddNewVersion(version *valueVersion) {
	manager.versions = append(manager.versions, version)
}

func (manager *versionManager) Set(ctx context.Context, value interface{}, txID int) {
	manager.rwMutex.Lock()
	defer manager.rwMutex.Unlock()
	manager.AddNewVersion(newSetValueVersion(value, txID))
}

func (manager *versionManager) getCommitedInternal(ctx context.Context) interface{} {
	if len(manager.versions) == 0 {
		return nil
	}

	return manager.versions[len(manager.versions)-1].value
}

func (manager *versionManager) Delete(ctx context.Context, txID int) error {
	manager.rwMutex.Lock()
	defer manager.rwMutex.Unlock()

	if manager.getCommitedInternal(ctx) != nil {
		manager.AddNewVersion(newDeleteValueVersion(txID))
	} else {
		return appCommon.KeyDoesNotExist
	}
	return nil
}

func (manager *versionManager) GetCommitted(ctx context.Context) interface{} {
	manager.rwMutex.RLock()
	defer manager.rwMutex.RUnlock()

	return manager.getCommitedInternal(ctx)
}

func (manager *versionManager) GetValueBeforeTransaction(ctx context.Context, txID int) interface{} {
	manager.rwMutex.RLock()
	defer manager.rwMutex.RUnlock()

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

func (manager *versionManager) GetLatestVersionForKey(ctx context.Context) (int, error) {
	manager.rwMutex.RLock()
	defer manager.rwMutex.RUnlock()

	if len(manager.versions) == 0 {
		return 0, appCommon.KeyDoesNotExist
	}
	return manager.versions[len(manager.versions)-1].txID, nil
}

func (manager *versionManager) RemoveOldVersion(ctx context.Context) error {
	manager.rwMutex.Lock()
	defer manager.rwMutex.Unlock()

	current := time.Now()
	for i := range manager.versions {
		if current.Sub(manager.versions[i].createdAt) < appCommon.TransactionTimeout {
			manager.versions = manager.versions[i:]
		}
	}

	return nil
}
