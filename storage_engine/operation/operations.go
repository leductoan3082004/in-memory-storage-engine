package operation

import (
	"in-memory-storage-engine/appCommon"
	"sync"
)

const (
	SET = iota
	GET = iota
	DELETE
)

type KeyStore interface {
	Get(key string) interface{}
	Set(key string, value interface{})
	Delete(key string) error
	CheckIfKeyExists(key string) bool
	GetAllOperation() *map[string]Operation
}

type Operation struct {
	OperationType int
	Value         interface{}
}

type operationsKeyStore struct {
	operationStore map[string]Operation
	rw             *sync.RWMutex
}

func newSetOperation(value interface{}) Operation {
	return Operation{
		OperationType: SET,
		Value:         value,
	}
}

func newDeleteOperation() Operation {
	return Operation{
		OperationType: DELETE,
		Value:         nil,
	}
}
func NewOperationsKeyStore() KeyStore {
	return operationsKeyStore{
		operationStore: make(map[string]Operation),
		rw:             new(sync.RWMutex),
	}
}

func (s operationsKeyStore) CheckIfKeyExists(key string) bool {
	s.rw.RLock()
	defer s.rw.RUnlock()
	_, ok := s.operationStore[key]
	return ok
}

func (s operationsKeyStore) newGetOperation(value interface{}) Operation {
	return Operation{
		OperationType: GET,
		Value:         value,
	}
}

func (s operationsKeyStore) newSetOperation(value interface{}) Operation {
	return Operation{
		OperationType: DELETE,
	}
}

func (s operationsKeyStore) Delete(key string) error {
	if !s.CheckIfKeyExists(key) {
		return appCommon.KeyDoesNotExist
	}
	s.rw.Lock()
	defer s.rw.Unlock()
	s.operationStore[key] = newDeleteOperation()
	return nil
}

func (s operationsKeyStore) Set(key string, value interface{}) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.operationStore[key] = newSetOperation(value)
}

func (s operationsKeyStore) GetAllOperation() *map[string]Operation {
	return &s.operationStore
}

func (s operationsKeyStore) Get(key string) interface{} {
	s.rw.RLock()
	defer s.rw.RUnlock()
	operation, exist := s.operationStore[key]
	if !exist || operation.OperationType == DELETE {
		return nil
	}
	return operation.Value
}
