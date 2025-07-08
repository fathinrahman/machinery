package common

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBConnector handles MongoDB connections
type MongoDBConnector struct {
	Client   *mongo.Client
	Database *mongo.Database
	Options  options.ClientOptions
}

func (mc *MongoDBConnector) Collection(name string) *mongo.Collection {
	return mc.Database.Collection(name)
}

// Connect creates a connected MongoWrapper
func (mc *MongoDBConnector) Connect(uri, dbName string, timeout time.Duration, opts options.ClientOptions) (*MongoDBConnector, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	client, err := mongo.Connect(ctx, opts.ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return &MongoDBConnector{
		Client:   client,
		Database: client.Database(dbName),
	}, nil
}

func (mc *MongoDBConnector) Close(ctx context.Context) error {
	if mc.Client != nil {
		return mc.Client.Disconnect(ctx)
	}

	return nil
}
