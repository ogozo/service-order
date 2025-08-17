package config

import (
	"reflect"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type OrderConfig struct {
	GRPCPort             string `mapstructure:"GRPC_PORT"`
	DatabaseURL          string `mapstructure:"DATABASE_URL"`
	RabbitMQURL          string `mapstructure:"RABBITMQ_URL"`
	OtelExporterEndpoint string `mapstructure:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OtelServiceName      string `mapstructure:"OTEL_SERVICE_NAME"`
	MetricsPort          string `mapstructure:"METRICS_PORT"`
}

func LoadConfig(cfg any) {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		envKey := field.Tag.Get("mapstructure")
		if envKey == "" {
			continue
		}

		err := viper.BindEnv(envKey)
		if err != nil {
			tempLogger, _ := zap.NewProduction()
			defer tempLogger.Sync()
			tempLogger.Fatal("Failed to bind env var", zap.String("key", envKey), zap.Error(err))
		}
	}

	err := viper.Unmarshal(cfg)
	if err != nil {
		tempLogger, _ := zap.NewProduction()
		defer tempLogger.Sync()
		tempLogger.Fatal("Unable to decode config into struct", zap.Error(err))
	}
}
