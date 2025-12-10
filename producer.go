package gw_kafka_client

import (
	"gw-kafka-client/internal/config"
	"gw-kafka-client/internal/producer"

	"go.uber.org/zap"
)

type Producer struct {
	Topics producer.Topics
}

func NewProducer(config config.Config, logger *zap.Logger) Producer {
	p := Producer{
		Topics: producer.GetTopics(config, logger),
	}
	return p
}
