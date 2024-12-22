package config

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ConnectMongo(ctx context.Context) (*mongo.Client, *mongo.Database, error) {
	clientOptions := options.Client().ApplyURI("mongodb://root:example@localhost:27017/?authSource=admin")
	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, nil, err
	}
	if err = mongoClient.Ping(ctx, nil); err != nil {
		return nil, nil, err
	}
	db := mongoClient.Database("teste")
	return mongoClient, db, nil
}
