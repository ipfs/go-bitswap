package lu

import (
	"fmt"
	"os"
	"path/filepath"

	logging "github.com/ipfs/go-log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log = logging.Logger("bs:sess")
var sflog = log.Desugar()

var speclog *zap.SugaredLogger

func init() {
	zapCfg := zap.NewProductionConfig()
	zapCfg.Encoding = "console"
	zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	zapCfg.Sampling = nil
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	if logfp := os.Getenv("BS_SPECIAL_LOG_FILE"); len(logfp) > 0 {
		if path, err := filepath.Abs(logfp); err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve log path '%q': %s\n", logfp, err)
		} else {
			zapCfg.OutputPaths = []string{path}
		}
	}

	cfg := zap.Config(zapCfg)
	cfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	newlog, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	speclog = newlog.Named("bs:special").Sugar()
}

func GetSpecialLogger() *zap.SugaredLogger {
	return speclog
}
