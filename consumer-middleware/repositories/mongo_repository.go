package repositories

import (
	"consumer-middleware/models"
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

func InsertVehicleMongo(ctx context.Context, coll *mongo.Collection, v models.Vehicle) error {
	_, err := coll.InsertOne(ctx, v)
	return err
}

func InsertAppointmentMongo(ctx context.Context, coll *mongo.Collection, a models.Appointment) error {
	_, err := coll.InsertOne(ctx, a)
	return err
}
