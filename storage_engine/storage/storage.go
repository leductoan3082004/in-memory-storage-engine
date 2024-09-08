package storage

import (
	"context"
	"github.com/sirupsen/logrus"
	"in-memory-storage-engine/appCommon"
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
	AbortTransaction(ctx context.Context, txID int) error
	CommitTransaction(ctx context.Context, txID int) error
}

var globalTransactionCount = 0

type memStore struct {
	data                      map[string]version.VersionManager
	affectedKeysInTransaction map[int]OperationKeyStore
	rwLock                    *sync.RWMutex
	logger                    *logrus.Logger
}

func NewMemStore() MemStorage {
	globalTransactionCount = 0
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors: true,
	})

	return &memStore{
		data:                      make(map[string]version.VersionManager),
		rwLock:                    new(sync.RWMutex),
		affectedKeysInTransaction: make(map[int]OperationKeyStore),
		logger:                    logger,
	}
}

func (s *memStore) Set(ctx context.Context, key string, value interface{}) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	increaseGlobalTransactionCount()
	s.setInternal(ctx, key, value, globalTransactionCount)
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
	increaseGlobalTransactionCount()

	return s.deleteInternal(ctx, key, globalTransactionCount)
}

func (s *memStore) makeMapOperationIfNotExist(txID int) {
	_, exist := s.affectedKeysInTransaction[txID]
	if !exist {
		s.affectedKeysInTransaction[txID] = NewOperationsKeyStore()
	}
}

func (s *memStore) StartTransaction(ctx context.Context) int {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	increaseGlobalTransactionCount()
	s.makeMapOperationIfNotExist(globalTransactionCount)
	s.logger.Infof("Transaction %d starts", globalTransactionCount)
	return globalTransactionCount
}

func (s *memStore) AbortTransaction(ctx context.Context, txID int) error {
	if !s.checkTxExistWithLock(txID) {
		s.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return appCommon.NewTxIDDoesNotExistError(txID)
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	delete(s.affectedKeysInTransaction, txID)
	return nil
}

func (s *memStore) CommitTransaction(ctx context.Context, txID int) error {
	if !s.checkTxExistWithLock(txID) {
		s.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return appCommon.NewTxIDDoesNotExistError(txID)
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	s.logger.Infof("Transaction %d is being commited...", txID)
	if err := s.checkIfTransactionCanBeCommited(ctx, txID); err != nil {
		s.logger.WithContext(ctx).Errorln(err)
		return err
	}
	s.logger.Infof("Applying transaction %d", txID)
	if err := s.applyTransaction(ctx, txID); err != nil {
		s.logger.WithContext(ctx).Errorln(err)
		return err
	}
	s.logger.Infof("Transaction %d is successfully committed", txID)
	delete(s.affectedKeysInTransaction, txID)
	return nil
}

func (s *memStore) GetValueForTransaction(ctx context.Context, txID int, key string) (interface{}, error) {
	if !s.checkTxExistWithLock(txID) {
		s.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return nil, appCommon.NewTxIDDoesNotExistError(txID)
	}

	value := s.affectedKeysInTransaction[txID].Get(key)
	if value != nil {
		return value, nil
	}

	if s.checkKeyExist(key) {
		return s.data[key].GetValueBeforeTransaction(ctx, txID), nil
	}

	return nil, nil
}

func (s *memStore) SetValueForTransaction(ctx context.Context, txID int, key string, value interface{}) error {
	if !s.checkTxExistWithLock(txID) {
		s.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return appCommon.NewTxIDDoesNotExistError(txID)
	}

	s.affectedKeysInTransaction[txID].Set(key, value)

	s.logger.Infof("Setting key %s with Value %v", key, value)
	return nil
}

func (s *memStore) DeleteValueForTransaction(ctx context.Context, txID int, key string) error {
	if !s.checkTxExistWithLock(txID) {
		s.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return appCommon.NewTxIDDoesNotExistError(txID)
	}

	// TODO: first need to check if key has been in transaction before or has been in current transaction
	if !s.affectedKeysInTransaction[txID].CheckIfKeyExists(key) {
		if !s.checkKeyExist(key) { // check if the key has been existed before
			s.logger.WithContext(ctx).Errorln(appCommon.KeyDoesNotExist)
			return appCommon.KeyDoesNotExist
		}

		// if it exists then check for if it has been deleted (since we store multiple versions)
		if s.data[key].GetValueBeforeTransaction(ctx, txID) != nil {
			s.logger.WithContext(ctx).Errorln(appCommon.KeyDoesNotExist)
			return appCommon.KeyDoesNotExist
		}
	}

	s.logger.Infof("Deleting key %s for transaction %d", key, txID)

	if err := s.affectedKeysInTransaction[txID].Delete(key); err != nil {
		s.logger.WithContext(ctx).Errorln(err)
		return err
	}

	return nil
}
