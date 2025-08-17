package healthcheck

import (
	"context"
	"fmt"
	"time"

	"github.com/ogozo/service-order/internal/logging"
	"go.uber.org/zap"
)

func ConnectWithRetry(ctx context.Context, operationName string, maxRetries int, baseDelay time.Duration, connectFunc func() error) {
	logger := logging.FromContext(ctx)

	for i := 0; i < maxRetries; i++ {
		err := connectFunc()
		if err == nil {
			logger.Info(fmt.Sprintf("Successfully connected to %s", operationName))
			return
		}

		delay := baseDelay * time.Duration(1<<i)
		logger.Warn(
			fmt.Sprintf("Could not connect to %s, retrying in %v", operationName, delay),
			zap.Error(err),
			zap.Int("attempt", i+1),
		)
		time.Sleep(delay)
	}

	logger.Fatal(
		fmt.Sprintf("Could not connect to %s after %d attempts", operationName, maxRetries),
	)
}
