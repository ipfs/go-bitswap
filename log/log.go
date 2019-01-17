package log

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"reflect"
	"runtime"

	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-loggables"
	goLogging "github.com/whyrusleeping/go-logging"
)

const (
	envLogTraces = "IPFS_BITSWAP_LOG_TRACES"
)

// Logger returns a logger for bitswap compatible with go-log EventLogger
// interface.
// In normal instances, it simply calls go-log's Logger constructor
// However, it can also return a wrapped logger that outputs tracing calls
// directly into regular logs.
// The specific purpose of this is the desire to integrate tracing into
// Bitswap while also aware there deployments which lack the infrastructure
// to make tracing useful, and we want to product comprehensible log messages
// instead
func Logger(name string) logging.EventLogger {

	log := logging.Logger(name)
	if os.Getenv(envLogTraces) == "" {
		return log
	}
	// this is a terrible hack to get at the underlying
	// go-logging logger if present to set the call depth
	// so file lines print correctly
	// it uses way more of the reflect api that one should ever
	// have access to!
	elem := reflect.ValueOf(log).Elem()
	typeOfT := elem.Type()
FieldIteration:
	for i := 0; i < elem.NumField(); i++ {
		if typeOfT.Field(i).Name == "StandardLogger" {
			standardLogger := elem.Field(i).Interface()
			goLogger, ok := standardLogger.(*goLogging.Logger)
			if ok {
				goLogger.ExtraCalldepth = 1
			}
			break FieldIteration
		}
	}
	return &bsLogWrapper{
		internalLog: log,
	}
}

type bsLogWrapper struct {
	internalLog logging.EventLogger
}

func (lw *bsLogWrapper) Debug(args ...interface{}) {
	lw.internalLog.Debug(args...)
}

func (lw *bsLogWrapper) Debugf(format string, args ...interface{}) {
	lw.internalLog.Debugf(format, args...)
}

func (lw *bsLogWrapper) Error(args ...interface{}) {
	lw.internalLog.Error(args...)
}

func (lw *bsLogWrapper) Errorf(format string, args ...interface{}) {
	lw.internalLog.Errorf(format, args...)
}

func (lw *bsLogWrapper) Fatal(args ...interface{}) {
	lw.internalLog.Fatal(args...)
}

func (lw *bsLogWrapper) Fatalf(format string, args ...interface{}) {
	lw.internalLog.Fatalf(format, args...)
}

func (lw *bsLogWrapper) Info(args ...interface{}) {
	lw.internalLog.Info(args...)
}

func (lw *bsLogWrapper) Infof(format string, args ...interface{}) {
	lw.internalLog.Infof(format, args...)
}

func (lw *bsLogWrapper) Panic(args ...interface{}) {
	lw.internalLog.Panic(args...)
}

func (lw *bsLogWrapper) Panicf(format string, args ...interface{}) {
	lw.internalLog.Panicf(format, args...)
}

func (lw *bsLogWrapper) Warning(args ...interface{}) {
	lw.internalLog.Warning(args...)
}

func (lw *bsLogWrapper) Warningf(format string, args ...interface{}) {
	lw.internalLog.Warningf(format, args...)
}

func (lw *bsLogWrapper) Start(ctx context.Context, name string) context.Context {
	newLoggable := loggables.Uuid(name)
	return logging.ContextWithLoggable(ctx, newLoggable)
}

func (lw *bsLogWrapper) StartFromParentState(ctx context.Context, name string, parent []byte) (context.Context, error) {
	metadata := logging.Metadata(make(map[string]interface{}))
	err := json.Unmarshal(parent, &metadata)
	if err != nil {
		return nil, err
	}

	newLoggable := loggables.Uuid(name)
	mergedLoggable := logging.DeepMerge(metadata, logging.Metadata(newLoggable.Loggable()))
	return logging.ContextWithLoggable(ctx, mergedLoggable), nil
}

func (lw *bsLogWrapper) SerializeContext(ctx context.Context) ([]byte, error) {
	metadata, err := logging.MetadataFromContext(ctx)
	if err != nil {
		return nil, err
	}
	return json.Marshal(metadata)
}

func (lw *bsLogWrapper) LogKV(ctx context.Context, alternatingKeyValues ...interface{}) {
	metadata, err := logging.MetadataFromContext(ctx)
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		lw.internalLog.Errorf("LogKV with no Span in context called on %s:%d", path.Base(file), line)
		return
	}
	metadata = logging.DeepMerge(logging.Metadata(make(map[string]interface{})), metadata)
	for i := 0; i*2 < len(alternatingKeyValues); i++ {
		key := alternatingKeyValues[i*2].(string)
		metadata[key] = alternatingKeyValues[i*2+1]
	}
	logMessage, err := metadata.JsonString()
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		lw.internalLog.Errorf("LogKV -- error serializing message", path.Base(file), line)
		return
	}
	lw.internalLog.Debug(logMessage)
}

func (lw *bsLogWrapper) SetTag(ctx context.Context, k string, v interface{}) {
	metadata, err := logging.MetadataFromContext(ctx)
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		lw.internalLog.Errorf("SetTag with no Span in context called on %s:%d", path.Base(file), line)
		return
	}
	metadata[k] = v
}

func (lw *bsLogWrapper) SetTags(ctx context.Context, tags map[string]interface{}) {
	metadata, err := logging.MetadataFromContext(ctx)
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		lw.internalLog.Errorf("SetTags with no Span in context called on %s:%d", path.Base(file), line)
		return
	}
	for k, v := range tags {
		metadata[k] = v
	}
}

func (lw *bsLogWrapper) setErr(ctx context.Context, err error, skip int) {
	_, merr := logging.MetadataFromContext(ctx)
	if merr != nil {
		_, file, line, _ := runtime.Caller(skip)
		lw.internalLog.Errorf("SetErr with no Span in context called on %s:%d", path.Base(file), line)
		return
	}
	if err == nil {
		return
	}
	lw.LogKV(ctx, "error", err.Error())
}

func (lw *bsLogWrapper) SetErr(ctx context.Context, err error) {
	lw.setErr(ctx, err, 1)
}

func (lw *bsLogWrapper) FinishWithErr(ctx context.Context, err error) {
	lw.setErr(ctx, err, 2)
}

func (lw *bsLogWrapper) Finish(ctx context.Context) {
}

func (lw *bsLogWrapper) Event(ctx context.Context, event string, m ...logging.Loggable) {
	lw.internalLog.Panicf("Do Not Use Deprecated Event Method")
}

func (lw *bsLogWrapper) EventBegin(ctx context.Context, event string, m ...logging.Loggable) *logging.EventInProgress {
	lw.internalLog.Panicf("Do Not Use Deprecated EventBegin Method")
	return nil
}
