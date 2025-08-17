package config

import (
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
	viper.AddConfigPath(".")
	viper.SetConfigName(".env")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		tempLogger, _ := zap.NewProduction()
		defer tempLogger.Sync()
		tempLogger.Warn(".env file not found, reading from environment variables")
	}

	err := viper.Unmarshal(&cfg)
	if err != nil {
		tempLogger, _ := zap.NewProduction()
		defer tempLogger.Sync()
		tempLogger.Fatal("Unable to decode config into struct", zap.Error(err))
	}
}
