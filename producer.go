package gw_event_bus

import (
	"github.com/execaus/gw-event-bus/internal"
	"github.com/execaus/gw-event-bus/internal/producer"
	"go.uber.org/zap"
)

type Producer struct {
	Topics producer.Topics
}

func NewProducer(host, port string, logger *zap.Logger) Producer {
	internal.Ping(host, port, logger)

	p := Producer{
		Topics: producer.GetTopics(host, port, logger),
	}

	return p
}
