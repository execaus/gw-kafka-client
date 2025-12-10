package gw_event_bus

import (
	"github.com/execaus/gw-event-bus/internal"
	"github.com/execaus/gw-event-bus/internal/consumer"

	"go.uber.org/zap"
)

type Consumer struct {
	Topics consumer.Topics
}

func NewConsumer(host, port string, logger *zap.Logger) Consumer {
	internal.Ping(host, port, logger)

	c := Consumer{
		Topics: consumer.GetTopics(host, port, logger),
	}
	return c
}
