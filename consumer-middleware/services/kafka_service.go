package services

import (
	"context"
	"database/sql"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/mongo"
)

func StartKafkaConsumer(ctx context.Context, consumer *kafka.Consumer, dbMongo *mongo.Database, pgDB *sql.DB) error {
	vehicleCollection := dbMongo.Collection("vehicles")
	appointmentCollection := dbMongo.Collection("appointments")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Erro ao ler mensagem: %v", err)
			continue
		}

		topic := *msg.TopicPartition.Topic
		log.Printf("Mensagem recebida do tópico %s: %s", topic, string(msg.Value))

		switch topic {
		case "br.com.brandao.vehicle":
			err := ProcessVehicleMessage(ctx, msg.Value, vehicleCollection, pgDB)
			if err != nil {
				log.Printf("Erro ao processar mensagem de vehicle: %v", err)
			}

		case "br.com.brandao.appointment":
			err := ProcessAppointmentMessage(ctx, msg.Value, appointmentCollection, pgDB)
			if err != nil {
				log.Printf("Erro ao processar mensagem de appointment: %v", err)
			}

		default:
			log.Printf("Tópico não reconhecido: %s", topic)
		}
	}
}
