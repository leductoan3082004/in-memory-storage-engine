package appCommon

import "fmt"

var (
	KeyDoesNotExist = fmt.Errorf("key does not exist")
)

func NewTxIDDoesNotExistError(txID int) error {
	return fmt.Errorf("transaction %d does not exist", txID)
}

func NewTxIDCanNotBeCommited(txID int) error {
	return fmt.Errorf("transaction %d cannot be committed", txID)
}
