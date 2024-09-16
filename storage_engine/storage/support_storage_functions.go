package storage

import (
	"context"
	"in-memory-storage-engine/appCommon"
	"in-memory-storage-engine/storage_engine/operation"
	"in-memory-storage-engine/storage_engine/version"
)

func increaseGlobalTransactionCount() {
	globalTransactionCount++
}

func (s *memStore) checkKeyExist(key string) bool {
	_, exist := s.data[key]
	return exist
}

func (s *memStore) checkKeyExistInTransaction(txID int, key string) (bool, error) {
	if !s.checkTxExist(txID) {
		s.logger.Errorln(appCommon.NewTxIDDoesNotExistError(txID))
		return false, appCommon.NewTxIDDoesNotExistError(txID)
	}
	return s.affectedKeysInTransaction[txID].CheckIfKeyExists(key), nil
}

func (s *memStore) checkTxExist(txID int) bool {
	_, exist := s.affectedKeysInTransaction[txID]
	return exist
}

func (s *memStore) checkTxExistWithLock(txID int) bool {
	s.writer.Lock()
	defer s.writer.Unlock()
	_, exist := s.affectedKeysInTransaction[txID]
	return exist
}

func (s *memStore) setInternal(ctx context.Context, key string, value interface{}, txID int) {
	if !s.checkKeyExist(key) {
		s.data[key] = version.NewValueVersionManager()
	}
	s.data[key].Set(ctx, value, txID)
}

func (s *memStore) deleteInternal(ctx context.Context, key string, txID int) error {
	if !s.checkKeyExist(key) {
		s.logger.WithContext(ctx).Errorln(appCommon.KeyDoesNotExist)
		return appCommon.KeyDoesNotExist
	}
	return s.data[key].Delete(ctx, txID)
}

func (s *memStore) checkIfTransactionCanBeCommited(ctx context.Context, txID int) error {
	for key, _ := range *s.affectedKeysInTransaction[txID].GetAllOperation() {
		if s.checkKeyExist(key) {
			keyTxID, err := s.data[key].GetLatestVersionForKey(ctx)
			if err != nil {
				s.logger.WithContext(ctx).Errorln(err)
				return err
			}
			if keyTxID > txID {
				return appCommon.NewTxIDCanNotBeCommited(txID)
			}
		}
	}
	return nil
}

func (s *memStore) applyTransaction(ctx context.Context, txID int) error {
	increaseGlobalTransactionCount()
	for key, value := range *s.affectedKeysInTransaction[txID].GetAllOperation() {
		switch value.OperationType {
		case operation.DELETE:
			_ = s.deleteInternal(ctx, key, globalTransactionCount)
			continue
		case operation.SET:
			s.setInternal(ctx, key, value.Value, globalTransactionCount)
			continue
		}
	}
	return nil
}
