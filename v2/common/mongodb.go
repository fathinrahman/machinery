package common

import (
	"context"

	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDBConnector struct{}

// Connect creates a MongoDB client and establishes a connection using `config.Config`
func (mc *MongoDBConnector) Connect(cnf *config.Config) (*mongo.Client, *mongo.Database, error) {
	// Create MongoDB client options by applying the URI and existing options
	uri := cnf.Broker
	clientOptions := cnf.MongoDB.Options.ApplyURI(uri)

	// Create MongoDB client and establish a connection
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.ERROR.Printf("Failed to connect to MongoDB at %s: %v", uri, err)
		return nil, nil, err
	}

	// Ensure connection is established
	if err := client.Ping(context.Background(), nil); err != nil {
		log.ERROR.Printf("Failed to ping MongoDB at %s: %v", uri, err)
		return nil, nil, err
	}

	// Set database
	database := client.Database(cnf.MongoDB.Database)

	return client, database, nil
}

// Close disconnects the MongoDB client
func (mc *MongoDBConnector) Close(ctx context.Context, client *mongo.Client) error {
	if client != nil {
		return client.Disconnect(ctx)
	}

	return nil
}
