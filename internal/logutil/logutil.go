package logutil

import (
	"io/ioutil"
	"os"

	logging "github.com/ipfs/go-log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var loggers = make(map[string]*BSLogger)
var logAll bool

type BSLogger struct {
	*logging.ZapEventLogger
	unsugared *zap.Logger
}

func (bsl *BSLogger) IsDebug() bool {
	if logAll {
		return true
	}
	return bsl.unsugared.Check(zap.DebugLevel, "debug log") != nil
}

func CreateLogger(name string) *BSLogger {
	logger := logging.Logger(name)
	bsLogger := &BSLogger{logger, logger.Desugar()}
	loggers[name] = bsLogger
	return bsLogger
}

func TeeLogs() *os.File {
	tmpfile, err := ioutil.TempFile("", "tee.log")
	if err != nil {
		panic(err)
	}

	logAll = true

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	writer := zapcore.AddSync(tmpfile)

	fileLogger := zapcore.NewCore(encoder, writer, zapcore.DebugLevel)

	for name, logger := range loggers {
		core := logger.Desugar().Core()
		teed := zapcore.NewTee(core, fileLogger)

		combinedLogger := &logging.ZapEventLogger{
			SugaredLogger: *zap.New(teed).Sugar(),
		}
		bsLogger := &BSLogger{combinedLogger, combinedLogger.Desugar()}

		existing := loggers[name]
		*existing = *bsLogger
	}

	return tmpfile
}
