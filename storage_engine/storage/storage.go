package storage

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"in-memory-storage-engine/appCommon"
	"in-memory-storage-engine/storage_engine/operation"
	"in-memory-storage-engine/storage_engine/version"
	"sync"
)

type MemStorage interface {
	Set(ctx context.Context, key string, value interface{}) error
	Get(ctx context.Context, key string) (interface{}, error)
	Delete(ctx context.Context, key string) error
	RemoveOldVersionTransaction(ctx context.Context) error
	Tx() MemTx
}

var globalTransactionCount = 0

type memStore struct {
	data                      map[string]version.VersionManager
	affectedKeysInTransaction map[int]operation.KeyStore
	writer                    *sync.Mutex
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
		writer:                    new(sync.Mutex),
		affectedKeysInTransaction: make(map[int]operation.KeyStore),
		logger:                    logger,
	}
}

func (s *memStore) Tx() MemTx {
	s.writer.Lock()
	defer s.writer.Unlock()
	increaseGlobalTransactionCount()
	s.makeMapOperationIfNotExist(globalTransactionCount)
	s.logger.Infof("Transaction %d starts", globalTransactionCount)

	return &memTx{
		memStore: s,
		txID:     globalTransactionCount,
	}
}

func (s *memStore) Set(ctx context.Context, key string, value interface{}) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	increaseGlobalTransactionCount()
	s.setInternal(ctx, key, value, globalTransactionCount)
	return nil
}

func (s *memStore) Get(ctx context.Context, key string) (interface{}, error) {
	if !s.checkKeyExist(key) {
		return nil, appCommon.KeyDoesNotExist
	}
	return s.data[key].GetCommitted(ctx), nil
}

func (s *memStore) Delete(ctx context.Context, key string) error {
	s.writer.Lock()
	defer s.writer.Unlock()
	increaseGlobalTransactionCount()

	return s.deleteInternal(ctx, key, globalTransactionCount)
}

func (s *memStore) makeMapOperationIfNotExist(txID int) {
	_, exist := s.affectedKeysInTransaction[txID]
	if !exist {
		s.affectedKeysInTransaction[txID] = operation.NewOperationsKeyStore()
	}
}

func (s *memStore) RemoveOldVersionTransaction(ctx context.Context) error {
	s.writer.Lock()
	defer s.writer.Unlock()

	for key, _ := range s.data {
		if err := s.data[key].RemoveOldVersion(ctx); err != nil {
			s.logger.WithContext(ctx).Errorln(err)
			return fmt.Errorf("there are some errors when running clean up process: %w", err)
		}
	}

	return nil
}
