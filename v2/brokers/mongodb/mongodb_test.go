package mongodb

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/tasks"
)

const (
	queueName = "test_queue"
	dbName    = "machinery_test"
	taskColl  = "task"
	lockColl  = "lock"
)

type mockProcessor struct {
	called chan *tasks.Signature
}

func (m *mockProcessor) Process(task *tasks.Signature) error {
	m.called <- task
	return nil
}

func (m *mockProcessor) CustomQueue() string {
	return ""
}

func (m *mockProcessor) PreConsumeHandler() bool {
	return true
}

// utility: creates a test mongo config (adjust URI as needed)
func testMongoConfig() *config.Config {
	uri := "mongodb://mongo:mongo@localhost:27017/?authSource=admin&readPreference=primary"
	return &config.Config{
		Broker:        uri,
		ResultBackend: uri,
		DefaultQueue:  queueName,
		MongoDB: &config.MongoDBConfig{
			Database: dbName,
			Options:  *options.Client().SetTimeout(3 * time.Second),
		},
	}
}

// utility: cleanup collections
func cleanupCollections(t *testing.T, broker *Broker, collections ...string) {
	client, db, _ := broker.MongoDBConnector.Connect(testMongoConfig())
	for _, coll := range collections {
		if err := db.Collection(coll).Drop(context.Background()); err != nil {
			t.Logf("Failed to drop collection %s: %v", coll, err)
		}
	}

	broker.MongoDBConnector.Close(context.Background(), client)
}

func TestMongoBroker_PublishAndConsume(t *testing.T) {
	cnf := testMongoConfig()
	brokerIface, err := New(
		cnf,
		queueName,
		taskColl,
		lockColl,
		3*time.Second,
	)
	if err != nil {
		t.Fatalf("New broker failed: %v", err)
	}
	broker := brokerIface.(*Broker)
	defer func() {
		cleanupCollections(t, broker, taskColl, lockColl)
	}()

	processor := &mockProcessor{called: make(chan *tasks.Signature, 1)}

	broker.SetRegisteredTaskNames([]string{"mock_task"})
	sig := &tasks.Signature{
		Name: "mock_task",
		Args: []tasks.Arg{{Type: "string", Value: "test"}},
	}
	if err := broker.Publish(context.Background(), sig); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	go func() {
		broker.StartConsuming("test_worker", 1, processor)
	}()

	select {
	case got := <-processor.called:
		if got.Name != "mock_task" {
			t.Errorf("Expected Name=mock_task, got %s", got.Name)
		} else {
			t.Logf("Task processed successfully: %s", got.Name)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Task not processed within timeout")
	}
	broker.StopConsuming()
}

func TestMongoBroker_RecoveryLockLifecycle(t *testing.T) {
	cnf := testMongoConfig()
	brokerIface, err := New(
		cnf,
		queueName,
		taskColl,
		lockColl,
		3*time.Second,
	)
	if err != nil {
		t.Fatalf("New broker failed: %v", err)
	}
	broker := brokerIface.(*Broker)
	defer func() {
		cleanupCollections(t, broker, taskColl, lockColl)
	}()

	// Init lock (should succeed, and be idempotent)
	if err := broker.initRecoveryLock(); err != nil {
		t.Fatalf("initRecoveryLock failed: %v", err)
	}
	// Try again to make sure it's idempotent
	if err := broker.initRecoveryLock(); err != nil {
		t.Fatalf("initRecoveryLock should be idempotent: %v", err)
	}

	// Try to acquire lock
	ok := broker.lockRecovery()
	if !ok {
		t.Error("lockRecovery should succeed on first attempt")
	}
	// Try again immediately (should not acquire because it's locked)
	ok2 := broker.lockRecovery()
	if ok2 {
		t.Error("lockRecovery should not acquire while already locked")
	}
	// Release lock
	broker.unlockRecovery()
	// Should be acquirable again
	ok3 := broker.lockRecovery()
	if !ok3 {
		t.Error("lockRecovery should be acquirable after unlock")
	}
	broker.unlockRecovery()
}

// Optionally, run this test only if you have a real mongo instance running
func TestMain(m *testing.M) {
	// Optionally, you can check for MongoDB up here
	os.Exit(m.Run())
}
