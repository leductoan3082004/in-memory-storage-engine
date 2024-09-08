package version

import "time"

type valueVersion struct {
	value     interface{}
	txID      int
	isVisible bool
	createdAt time.Time
}

type valueVersions []*valueVersion

func newSetValueVersion(value interface{}, txID int) *valueVersion {
	return &valueVersion{
		value:     value,
		txID:      txID,
		isVisible: true,
		createdAt: time.Now(),
	}
}

func newDeleteValueVersion(txID int) *valueVersion {
	return &valueVersion{
		txID:      txID,
		isVisible: false,
		createdAt: time.Now(),
	}
}
