package services

import (
	"consumer-middleware/config"
	"consumer-middleware/models"
	"consumer-middleware/repositories"
	"consumer-middleware/utils"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/mongo"
)

func ProcessMessage[T any](
	ctx context.Context,
	message []byte,
	mongoColl *mongo.Collection,
	pgDB *sql.DB,
	validate func(T) error,
	insertMongo func(context.Context, *mongo.Collection, T) error,
	insertPostgres func(*sql.DB, T) error,
	dltTopic string,
	successLog string,
) error {
	var data T

	err := utils.Retry(3, 1*time.Second, func() error {
		if err := json.Unmarshal(message, &data); err != nil {
			log.Printf("Erro ao decodificar JSON: %v", err)
			return err
		}
		if err := validate(data); err != nil {
			log.Printf("Validação falhou: %v", err)
			return err
		}
		return nil
	})

	if err != nil {
		log.Printf("Falha no processamento após múltiplas tentativas: %v", err)
		moveToDeadLetterTopic(message, dltTopic, err)
		return err
	}

	err = utils.Retry(3, 1*time.Second, func() error {
		if err := insertMongo(ctx, mongoColl, data); err != nil {
			log.Printf("Erro ao inserir no MongoDB: %v", err)
			return err
		}
		if err := insertPostgres(pgDB, data); err != nil {
			log.Printf("Erro ao inserir no PostgreSQL: %v", err)
			return err
		}
		return nil
	})

	if err != nil {
		log.Printf("Erro na persistência após múltiplas tentativas: %v", err)
		moveToDeadLetterTopic(message, dltTopic, err)
		return err
	}

	log.Println(successLog, data)
	return nil
}

func ProcessVehicleMessage(ctx context.Context, message []byte, mongoColl *mongo.Collection, pgDB *sql.DB) error {
	validate := func(v models.Vehicle) error {
		if v.ID == 0 || v.Plate == "" || v.Brand == "" || v.Modelo == "" {
			return errors.New("mensagem de vehicle contém campos obrigatórios ausentes")
		}
		return nil
	}

	return ProcessMessage[models.Vehicle](
		ctx,
		message,
		mongoColl,
		pgDB,
		validate,
		repositories.InsertVehicleMongo,
		repositories.InsertVehiclePostgres,
		"vehicle-dlt",
		"Veículo processado com sucesso:",
	)
}

func ProcessAppointmentMessage(ctx context.Context, message []byte, mongoColl *mongo.Collection, pgDB *sql.DB) error {
	validate := func(a models.Appointment) error {
		if a.ID == 0 || a.VehicleID == 0 || a.Lat == "" || a.Long == "" || a.ProcessDate == "" {
			return errors.New("mensagem de appointment contém campos obrigatórios ausentes")
		}
		return nil
	}

	return ProcessMessage[models.Appointment](
		ctx,
		message,
		mongoColl,
		pgDB,
		validate,
		repositories.InsertAppointmentMongo,
		repositories.InsertAppointmentPostgres,
		"appointment-dlt",
		"Appointment processado com sucesso:",
	)
}

func moveToDeadLetterTopic(message []byte, topic string, err error) {
	producer, errProducer := config.CreateKafkaProducer()
	if errProducer != nil {
		log.Printf("Erro ao criar produtor Kafka para DLT: %v", errProducer)
		return
	}
	defer producer.Close()

	dltMessage := map[string]interface{}{
		"message": string(message),
		"error":   err.Error(),
	}

	dltBytes, errMarshal := json.Marshal(dltMessage)
	if errMarshal != nil {
		log.Printf("Erro ao marshallizar mensagem para DLT: %v", errMarshal)
		return
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          dltBytes,
	}

	errProduce := producer.Produce(msg, nil)
	if errProduce != nil {
		log.Printf("Erro ao enviar mensagem para DLT (%s): %v", topic, errProduce)
	} else {
		log.Printf("Mensagem movida para Dead Letter Topic (%s): %s", topic, string(dltBytes))
	}

	producer.Flush(15 * 1000)
}
