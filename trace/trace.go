package trace

import (
	"context"
	"encoding/json"
	"fmt"
	logging "github.com/ipfs/go-log"
	"go.opencensus.io/trace"
)

var log = logging.Logger("bitswap")

type Span struct {
	span *trace.Span
}

type Attribute struct {
	Key   string		`json:"key"`
	Value interface{}	`json:"value"`
}

func Int64Attribute(key string, value int64) Attribute {
	return cookedAttribute(key, value)
}

func StringAttribute(key string, value string) Attribute {
	return cookedAttribute(key, value)
}

func BoolAttribute(key string, value bool) Attribute {
	return cookedAttribute(key, value)
}

func cookedAttribute(key string, value interface{}) Attribute {
	sValue := fmt.Sprintf("%v", value)
	return Attribute{key, sValue}
}

func StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	ctx, span := trace.StartSpan(ctx, name)

	return ctx, &Span{span}
}

func FromContext(ctx context.Context) *Span {
	s := trace.FromContext(ctx)
	return &Span{s}
}

func (span *Span) Annotate(attributes []Attribute, str string) {
	ta := traceAttributes(attributes)
	span.span.Annotate(ta, str)

	aMap := make(map[string]string)
	for _, a := range attributes {
		aMap[a.Key] = a.Value.(string)
	}

	aMap["dd.trace_id"] = fmt.Sprintf("%v", span.span.SpanContext().TraceID)
	aMap["dd.span_id"] = fmt.Sprintf("%v", span.span.SpanContext().SpanID)
	aMap["system"] = "bitswap"

	js, _ :=json.Marshal(aMap)
	log.Info(string(js))
}

func (span *Span) AddAttributes(attributes ...Attribute)() {
	span.span.AddAttributes(traceAttributes(attributes)...)
}

func (span *Span) End() {
	span.span.End()
}

func traceAttributes(attributes []Attribute) []trace.Attribute {
	var out []trace.Attribute
	for _, a := range attributes {
		out = append(out, trace.StringAttribute(a.Key, a.Value.(string)))
	}

	return out
}