package config

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ConnectKafka inicializa um consumidor Kafka
func ConnectKafka() (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "meu-grupo-consumidor-beto",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		return nil, err
	}
	err = consumer.SubscribeTopics([]string{"br.com.brandao.appointment", "br.com.brandao.vehicle"}, nil)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func CreateKafkaProducer() (*kafka.Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		return nil, err
	}
	return producer, nil
}
