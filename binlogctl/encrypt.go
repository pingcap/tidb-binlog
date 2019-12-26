package binlogctl

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pkg/encrypt"
	"go.uber.org/zap"
)

// EncryptHandler log the encrypted text if success or return error.
func EncryptHandler(text string) error {
	enc, err := encrypt.Encrypt(text)
	if err != nil {
		return err
	}

	log.Info("encrypt text", zap.String("encrypted", string(enc)), zap.String("origin", text))
	return nil
}
