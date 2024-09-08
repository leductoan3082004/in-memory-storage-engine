package storage

import "sync"

const (
	SET = iota
	GET = iota
	DELETE
)

type operations struct {
	operationType int
	value         interface{}
}

type operationsKeyStore struct {
	operationStore map[string]operations
	rw             *sync.RWMutex
}

func newSetOperation(value interface{}) operations {
	return operations{
		operationType: SET,
		value:         value,
	}
}

func newDeleteOperation() operations {
	return operations{
		operationType: DELETE,
		value:         nil,
	}
}
func newOperationsKeyStore() operationsKeyStore {
	return operationsKeyStore{
		operationStore: make(map[string]operations),
		rw:             new(sync.RWMutex),
	}
}
