package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const (
	connectTimeout = 5 * time.Second
)

func Ping(host, port string, logger *zap.Logger) {
	address := fmt.Sprintf("%s:%s", host, port)

	connCtx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	conn, err := kafka.DialContext(connCtx, "tcp", address)
	if err != nil {
		logger.Fatal("kafka ping failed", zap.String("address", address), zap.Error(err))
	} else {
		brokers, err := conn.Brokers()
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Debug("kafka ping successful", zap.String("address", address), zap.Any("brokers", brokers))
		if err = conn.Close(); err != nil {
			logger.Fatal(err.Error())
		}
	}
}
