package logutil

import (
	"io/ioutil"
	"os"

	logging "github.com/ipfs/go-log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type BSLogger struct {
	*logging.ZapEventLogger
	unsugared *zap.Logger
}

func (bsl *BSLogger) Check(level zapcore.Level, log string) *zapcore.CheckedEntry {
	return bsl.unsugared.Check(level, log)
}

var loggers = make(map[string]*logging.ZapEventLogger)

func CreateLogger(name string) *BSLogger {
	logger := logging.Logger(name)
	loggers[name] = logger
	return &BSLogger{logger, logger.Desugar()}
}

func TeeLogs() *os.File {
	tmpfile, err := ioutil.TempFile("", "tee.log")
	if err != nil {
		panic(err)
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	writer := zapcore.AddSync(tmpfile)

	fileLogger := zapcore.NewCore(encoder, writer, zapcore.DebugLevel)

	for name, logger := range loggers {
		core := logger.Desugar().Core()
		teed := zapcore.NewTee(core, fileLogger)

		logger := zap.New(teed).Sugar()
		combinedLogger := &logging.ZapEventLogger{
			SugaredLogger: *logger,
		}

		ptr := loggers[name]
		*ptr = *combinedLogger
	}

	return tmpfile
}
