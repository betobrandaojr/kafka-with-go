package main

import (
	"consumer-middleware/config"
	"consumer-middleware/services"
	"context"
	"log"
)

func main() {
	ctx := context.Background()

	log.Println("Iniciando conexão com MongoDB...")
	mongoClient, dbMongo, err := config.ConnectMongo(ctx)
	if err != nil {
		log.Fatalf("Erro ao conectar no MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)
	log.Println("Conexão com MongoDB estabelecida com sucesso!")

	log.Println("Iniciando conexão com PostgreSQL...")
	pgDB, err := config.ConnectPostgres()
	if err != nil {
		log.Fatalf("Erro ao conectar no PostgreSQL: %v", err)
	}
	defer pgDB.Close()
	log.Println("Conexão com PostgreSQL estabelecida com sucesso!")

	log.Println("Iniciando conexão com Kafka...")
	consumer, err := config.ConnectKafka()
	if err != nil {
		log.Fatalf("Erro ao conectar ao Kafka: %v", err)
	}
	defer consumer.Close()
	log.Println("Conexão com Kafka estabelecida com sucesso!")

	log.Println("Consumidor Kafka iniciado. Aguardando novas mensagens...")
	if err := services.StartKafkaConsumer(ctx, consumer, dbMongo, pgDB); err != nil {
		log.Fatalf("Erro ao processar mensagens Kafka: %v", err)
	}
}
