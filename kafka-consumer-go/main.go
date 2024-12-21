package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Vehicle struct {
	ID     int    `json:"id"`
	Plate  string `json:"plate"`
	Brand  string `json:"brand"`
	Modelo string `json:"modelo"`
}

type Appointment struct {
	ID          int    `json:"id"`
	VehicleID   int    `json:"vehicle_id"`
	Lat         string `json:"lat"`
	Long        string `json:"long"`
	ProcessDate string `json:"process_date"`
}

func main() {
	ctx := context.Background()

	mongoClient, err := connectMongo(ctx)
	if err != nil {
		log.Fatalf("Erro ao conectar no MongoDB: %v", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(ctx); err != nil {
			log.Fatalf("Erro ao desconectar do MongoDB: %v", err)
		}
	}()

	dbMongo := mongoClient.Database("teste")
	vehicleCollection := dbMongo.Collection("vehicles")
	appointmentCollection := dbMongo.Collection("appointments")
	fmt.Println("Conectado ao MongoDB com sucesso!")

	pgDB, err := connectPostgres()
	if err != nil {
		log.Fatalf("Erro ao conectar no PostgreSQL: %v", err)
	}
	defer pgDB.Close()
	fmt.Println("Conectado ao PostgreSQL com sucesso!")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "meu-grupo-consumidor-beto",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	err = c.SubscribeTopics([]string{"br.com.brandao.appointment", "br.com.brandao.vehicle"}, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("Esperando mensagens...")
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Erro ao ler mensagem: %v (%v)\n", err, msg)
			continue
		}

		topic := *msg.TopicPartition.Topic
		fmt.Printf("Mensagem recebida do tópico %s: %s\n", topic, string(msg.Value))

		switch topic {
		case "br.com.brandao.vehicle":
			var v Vehicle
			if err := json.Unmarshal(msg.Value, &v); err != nil {
				fmt.Printf("Erro ao decodificar JSON de vehicle: %v\n", err)
				continue
			}

			if err := insertVehicleMongo(ctx, vehicleCollection, v); err != nil {
				fmt.Printf("Erro ao inserir veículo no MongoDB: %v\n", err)
			} else {
				fmt.Println("Veículo inserido com sucesso no MongoDB!")
			}

			if err := insertVehiclePostgres(pgDB, v); err != nil {
				fmt.Printf("Erro ao inserir veículo no PostgreSQL: %v\n", err)
			} else {
				fmt.Println("Veículo inserido com sucesso no PostgreSQL!")
			}

		case "br.com.brandao.appointment":
			var a Appointment
			if err := json.Unmarshal(msg.Value, &a); err != nil {
				fmt.Printf("Erro ao decodificar JSON de appointment: %v\n", err)
				continue
			}

			if err := insertAppointmentMongo(ctx, appointmentCollection, a); err != nil {
				fmt.Printf("Erro ao inserir appointment no MongoDB: %v\n", err)
			} else {
				fmt.Println("Appointment inserido com sucesso no MongoDB!")
			}

			if err := insertAppointmentPostgres(pgDB, a); err != nil {
				fmt.Printf("Erro ao inserir appointment no PostgreSQL: %v\n", err)
			} else {
				fmt.Println("Appointment inserido com sucesso no PostgreSQL!")
			}
		}
	}
}

//------------------------------------------------------------------------------

func connectMongo(ctx context.Context) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI("mongodb://root:example@localhost:27017/?authSource=admin")
	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	if err = mongoClient.Ping(ctx, nil); err != nil {
		return nil, err
	}
	return mongoClient, nil
}

func connectPostgres() (*sql.DB, error) {
	connStr := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	pgDB, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	if err = pgDB.Ping(); err != nil {
		return nil, err
	}
	return pgDB, nil
}

//------------------------------------------------------------------------------

func insertVehicleMongo(ctx context.Context, coll *mongo.Collection, v Vehicle) error {
	_, err := coll.InsertOne(ctx, v)
	return err
}

func insertAppointmentMongo(ctx context.Context, coll *mongo.Collection, a Appointment) error {
	_, err := coll.InsertOne(ctx, a)
	return err
}

// ------------------------------------------------------------------------------
func insertVehiclePostgres(db *sql.DB, v Vehicle) error {
	query := `INSERT INTO _machines.vehicles (id, plate, brand, modelo) 
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (id) DO NOTHING;`
	_, err := db.Exec(query, v.ID, v.Plate, v.Brand, v.Modelo)
	return err
}

func insertAppointmentPostgres(db *sql.DB, a Appointment) error {
	query := `INSERT INTO _messages.appointments (id, vehicle_id, lat, long, process_date)
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT (id) DO NOTHING;`
	_, err := db.Exec(query, a.ID, a.VehicleID, a.Lat, a.Long, a.ProcessDate)
	return err
}
