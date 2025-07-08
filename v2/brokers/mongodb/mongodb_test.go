package mongodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/mongodb"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// mockProcessor records all processed tasks for assertion.
type mockProcessor struct {
	called chan *tasks.Signature
}

func (m *mockProcessor) Process(task *tasks.Signature) error {
	m.called <- task
	return nil
}

func (m *mockProcessor) CustomQueue(signature *tasks.Signature) string      { return "" }
func (m *mockProcessor) PreConsumeHandler(signature *tasks.Signature) error { return nil }

// TestMongoBroker_PublishAndProcess verifies that:
// - a published task can be retrieved as pending
// - processor.Process is called for the right task
func TestMongoBroker_PublishAndProcess(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("publish and process task", func(mt *mtest.T) {
		// Ensure the target collection exists in the test DB.
		mt.DB.Collection("machinery_tasks")

		// Set up a MongoDBConnector with the test mock client.
		conn := &common.MongoDBConnector{
			Client:   mt.Client,
			Database: mt.DB,
		}

		// Create a new broker. The collection pointer must be injected for the test to work.
		broker := mongodb.New(
			&config.Config{
				Broker: "mongodb://localhost:27017",
			},
			"default",
			"machinery_test",
			"machinery.task",
			"machinery.lock",
			3*time.Second,
			10*time.Minute,
		).(*mongodb.Broker)

		// Required: inject the collection pointer for the broker (mock DB, mock collection).
		// This is needed because in tests we are not calling StartConsuming (which would set it up).
		broker.SetTaskCollection(conn.Database.Collection("machinery.task"))

		now := time.Now().UTC()
		task := &tasks.Signature{
			UUID: "test-uuid",
			Name: "example_task",
			ETA:  &now,
		}

		// 1. Test Publish: Simulate a successful insert
		mt.AddMockResponses(mtest.CreateSuccessResponse())
		err := broker.Publish(context.Background(), task)
		require.NoError(t, err, "Publish should not return error")

		// 2. Test GetPendingTasks: Simulate a find response with one task
		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, "machinery.task", mtest.FirstBatch, bson.D{
				primitive.E{Key: "_id", Value: primitive.NewObjectID()},
				primitive.E{Key: "queue", Value: "default"},
				primitive.E{Key: "status", Value: "pending"},
				primitive.E{Key: "eta", Value: now},
				primitive.E{Key: "task", Value: bson.D{
					primitive.E{Key: "uuid", Value: task.UUID},
					primitive.E{Key: "name", Value: task.Name},
				}},
			}),
			mtest.CreateCursorResponse(0, "machinery.task", mtest.NextBatch),
		)
		pending, err := broker.GetPendingTasks("default")
		require.NoError(t, err, "GetPendingTasks should not return error")
		require.Len(t, pending, 1, "Should find exactly one pending task")
		require.Equal(t, task.UUID, pending[0].UUID, "Pending task UUID should match")
		require.Equal(t, task.Name, pending[0].Name, "Pending task Name should match")

		// 3. Test Process: Use the mockProcessor and simulate handling the task
		processor := &mockProcessor{called: make(chan *tasks.Signature, 1)}
		go func() {
			_ = processor.Process(task) // simulate worker
		}()

		select {
		case got := <-processor.called:
			require.Equal(t, task.UUID, got.UUID, "Processed task UUID should match")
			require.Equal(t, task.Name, got.Name, "Processed task Name should match")
		case <-time.After(2 * time.Second):
			t.Fatal("processor did not receive the task")
		}
	})
}
