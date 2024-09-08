package storage

import (
	"context"
	"in-memory-storage-engine/storage_engine/appCommon"
	"in-memory-storage-engine/storage_engine/version"
	"sync"
)

type MemStorage interface {
	Set(ctx context.Context, key string, value interface{})
	Get(ctx context.Context, key string) interface{}
	Delete(ctx context.Context, key string) error
	StartTransaction(ctx context.Context) int
	GetValueForTransaction(ctx context.Context, txID int, key string) (interface{}, error)
	SetValueForTransaction(ctx context.Context, txID int, key string, value interface{}) error
	DeleteValueForTransaction(ctx context.Context, txID int, key string) error
}

var globalTransactionCount = 0

type memStore struct {
	data                      map[string]version.VersionManager
	affectedKeysInTransaction map[int]operationsKeyStore
	rwLock                    *sync.RWMutex
}

func NewMemStore() MemStorage {
	return &memStore{
		data:                      make(map[string]version.VersionManager),
		rwLock:                    new(sync.RWMutex),
		affectedKeysInTransaction: make(map[int]operationsKeyStore),
	}
}

func increaseGlobalTransactionCount() {
	globalTransactionCount++
}

func (s *memStore) checkKeyExist(key string) bool {
	_, exist := s.data[key]
	return exist
}

func (s *memStore) checkKeyExistInTransaction(txID int, key string) (bool, error) {
	_, exist := s.affectedKeysInTransaction[txID]
	if !exist {
		return false, appCommon.NewTxIDDoesNotExistError(txID)
	}
	_, exist = s.affectedKeysInTransaction[txID].operationStore[key]
	return exist, nil
}

func (s *memStore) checkTxExist(txID int) bool {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	_, exist := s.affectedKeysInTransaction[txID]
	return exist
}

func (s *memStore) Set(ctx context.Context, key string, value interface{}) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	increaseGlobalTransactionCount()
	if !s.checkKeyExist(key) {
		s.data[key] = version.NewValueVersionManager()
	}
	s.data[key].Set(ctx, value, globalTransactionCount)
}

func (s *memStore) Get(ctx context.Context, key string) interface{} {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if !s.checkKeyExist(key) {
		return nil
	}
	return s.data[key].GetCommitted(ctx)
}

func (s *memStore) Delete(ctx context.Context, key string) error {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if !s.checkKeyExist(key) {
		return appCommon.KeyDoesNotExist
	}
	increaseGlobalTransactionCount()
	return s.data[key].Delete(ctx, globalTransactionCount)
}

func (s *memStore) makeMapOperationIfNotExist(txID int) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	_, exist := s.affectedKeysInTransaction[txID]
	if !exist {
		s.affectedKeysInTransaction[txID] = newOperationsKeyStore()
	}
}

func (s *memStore) StartTransaction(ctx context.Context) int {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	increaseGlobalTransactionCount()
	s.makeMapOperationIfNotExist(globalTransactionCount)
	return globalTransactionCount
}

func (s *memStore) GetValueForTransaction(ctx context.Context, txID int, key string) (interface{}, error) {
	if !s.checkTxExist(txID) {
		return nil, appCommon.NewTxIDDoesNotExistError(txID)
	}

	s.rwLock.RLock()
	defer s.rwLock.RUnlock()

	exist, err := s.checkKeyExistInTransaction(txID, key)
	if err != nil {
		return nil, err
	}

	if exist {
		return s.affectedKeysInTransaction[txID].operationStore[key].value, nil
	}
	return s.data[key].GetValueForTransaction(ctx, txID), nil
}

func (s *memStore) SetValueForTransaction(ctx context.Context, txID int, key string, value interface{}) error {
	if !s.checkTxExist(txID) {
		return appCommon.NewTxIDDoesNotExistError(txID)
	}

	s.affectedKeysInTransaction[txID].rw.Lock()
	defer s.affectedKeysInTransaction[txID].rw.Unlock()
	s.affectedKeysInTransaction[txID].operationStore[key] = newSetOperation(value)

	return nil
}

func (s *memStore) DeleteValueForTransaction(ctx context.Context, txID int, key string) error {
	if !s.checkTxExist(txID) {
		return appCommon.NewTxIDDoesNotExistError(txID)
	}

	s.affectedKeysInTransaction[txID].rw.Lock()
	defer s.affectedKeysInTransaction[txID].rw.Unlock()
	s.affectedKeysInTransaction[txID].operationStore[key] = newDeleteOperation()

	return nil
}
