package storage

import (
	"context"
	"in-memory-storage-engine/appCommon"
)

type MemTx interface {
	Set(ctx context.Context, key string, value interface{}) error
	Get(ctx context.Context, key string) (interface{}, error)
	Delete(ctx context.Context, key string) error
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
}

type memTx struct {
	memStore *memStore
	txID     int
}

func (tx *memTx) Abort(ctx context.Context) error {
	tx.memStore.writer.Lock()
	defer tx.memStore.writer.Unlock()

	if !tx.memStore.checkTxExist(tx.txID) {
		tx.memStore.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(tx.txID))
		return appCommon.NewTxIDDoesNotExistError(tx.txID)
	}

	tx.memStore.logger.Infof("Aborting transaction %d", tx.txID)

	delete(tx.memStore.affectedKeysInTransaction, tx.txID)
	tx.memStore.logger.Infof("Aborted transaction %d successfully", tx.txID)
	return nil
}

func (tx *memTx) Commit(ctx context.Context) error {
	tx.memStore.writer.Lock()
	defer tx.memStore.writer.Unlock()

	if !tx.memStore.checkTxExist(tx.txID) {
		tx.memStore.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(tx.txID))
		return appCommon.NewTxIDDoesNotExistError(tx.txID)
	}

	tx.memStore.logger.Infof("Transaction %d is being commited...", tx.txID)
	if err := tx.memStore.checkIfTransactionCanBeCommited(ctx, tx.txID); err != nil {
		tx.memStore.logger.WithContext(ctx).Errorln(err)
		return err
	}
	tx.memStore.logger.Infof("Applying transaction %d", tx.txID)
	if err := tx.memStore.applyTransaction(ctx, tx.txID); err != nil {
		tx.memStore.logger.WithContext(ctx).Errorln(err)
		return err
	}
	tx.memStore.logger.Infof("Transaction %d is successfully committed", tx.txID)
	delete(tx.memStore.affectedKeysInTransaction, tx.txID)
	return nil
}

func (tx *memTx) Set(ctx context.Context, key string, value interface{}) error {
	if !tx.memStore.checkTxExist(tx.txID) {
		tx.memStore.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(tx.txID))
		return appCommon.NewTxIDDoesNotExistError(tx.txID)
	}

	tx.memStore.affectedKeysInTransaction[tx.txID].Set(key, value)

	tx.memStore.logger.Infof("Setting key %s with Value %v for tracsaction %d", key, value, tx.txID)
	return nil
}

func (tx *memTx) Get(ctx context.Context, key string) (interface{}, error) {
	if !tx.memStore.checkTxExist(tx.txID) {
		tx.memStore.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(tx.txID))
		return nil, appCommon.NewTxIDDoesNotExistError(tx.txID)
	}

	value := tx.memStore.affectedKeysInTransaction[tx.txID].Get(key)
	if value != nil {
		return value, nil
	}

	if tx.memStore.checkKeyExist(key) {
		return tx.memStore.data[key].GetValueBeforeTransaction(ctx, tx.txID), nil
	}

	return nil, nil
}

func (tx *memTx) Delete(ctx context.Context, key string) error {
	if !tx.memStore.checkTxExist(tx.txID) {
		tx.memStore.logger.WithContext(ctx).Errorln(appCommon.NewTxIDDoesNotExistError(tx.txID))
		return appCommon.NewTxIDDoesNotExistError(tx.txID)
	}

	// TODO: first need to check if key has been in transaction before or has been in current transaction
	if !tx.memStore.affectedKeysInTransaction[tx.txID].CheckIfKeyExists(key) {
		if !tx.memStore.checkKeyExist(key) { // check if the key has been existed before
			tx.memStore.logger.WithContext(ctx).Errorln(appCommon.KeyDoesNotExist)
			return appCommon.KeyDoesNotExist
		}

		// if it exists then check for if it has been deleted (since we store multiple versions)
		if tx.memStore.data[key].GetValueBeforeTransaction(ctx, tx.txID) != nil {
			tx.memStore.logger.WithContext(ctx).Errorln(appCommon.KeyDoesNotExist)
			return appCommon.KeyDoesNotExist
		}
	}

	tx.memStore.logger.Infof("Deleting key %s for transaction %d", key, tx.txID)

	if err := tx.memStore.affectedKeysInTransaction[tx.txID].Delete(key); err != nil {
		tx.memStore.logger.WithContext(ctx).Errorln(err)
		return err
	}

	return nil
}
