package common

import (
	"context"
	"fmt"

	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBConnector struct{}

// Connect initializes and returns a MongoDB database handle based on the provided config.
// If a MongoDB client is already present in the config, it reuses that client.
// Otherwise, it creates a new client using the Broker URI, establishes a connection,
// and verifies connectivity by pinging the MongoDB server.
// Returns a *mongo.Database instance for the configured database or an error if the
// connection or ping fails.
func (mc *MongoDBConnector) Connect(cnf *config.Config) (*mongo.Database, error) {
	if cnf.MongoDB == nil || cnf.MongoDB.Database == "" {
		return nil, fmt.Errorf("database name is required")
	}

	if cnf.MongoDB.Client != nil {
		return cnf.MongoDB.Client.Database(cnf.MongoDB.Database), nil
	}

	// Create MongoDB client and establish a connection
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cnf.Broker))
	if err != nil {
		log.ERROR.Printf("failed to connect to MongoDB at %s: %v", cnf.Broker, err)
		return nil, err
	}

	// Ensure connection is established
	if err := client.Ping(context.Background(), nil); err != nil {
		log.ERROR.Printf("failed to ping MongoDB at %s: %v", cnf.Broker, err)
		return nil, err
	}

	return client.Database(cnf.MongoDB.Database), nil
}

// Close disconnects the MongoDB client
func (mc *MongoDBConnector) Close(ctx context.Context, client *mongo.Client) error {
	if client != nil {
		return client.Disconnect(ctx)
	}

	return nil
}
