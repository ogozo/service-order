package logging

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type contextKey struct{}
var loggerKey = contextKey{}

var globalLogger *zap.Logger

func Init(serviceName string) {
	config := zap.NewProductionConfig()
    
    config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := config.Build(zap.AddCaller())
	if err != nil {
		panic(err)
	}

	globalLogger = logger.With(zap.String("service", serviceName))
}

func Sync() {
	if globalLogger != nil {
		globalLogger.Sync()
	}
}

func FromContext(ctx context.Context) *zap.Logger {
	if l, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return l
	}
	return globalLogger
}

func WithContext(ctx context.Context, l *zap.Logger) context.Context {
    return context.WithValue(ctx, loggerKey, l)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	l := FromContext(ctx)
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		fields = append(fields,
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	l.Info(msg, fields...)
}

func Error(ctx context.Context, msg string, err error, fields ...zap.Field) {
	l := FromContext(ctx)
	fields = append(fields, zap.Error(err))
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		fields = append(fields,
			zap.String("trace_id", span.SpanContext().TraceID().String()),
			zap.String("span_id", span.SpanContext().SpanID().String()),
		)
	}
	l.Error(msg, fields...)
}