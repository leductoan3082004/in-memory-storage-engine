package cronjob

import (
	"context"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"in-memory-storage-engine/storage_engine/storage"
)

func RemoveOldVersionKey(store storage.MemStorage) error {
	c := cron.New()
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{ForceColors: true})

	_, err := c.AddFunc("*/5 * * * *", func() {
		if err := store.RemoveOldVersionTransaction(context.Background()); err != nil {
			logger.Errorln("clean up old transaction has some errors: %w", err)
		}
	})

	return err
}
