package mongodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/fathinrahman/machinery/v2/brokers/errs"
	"github.com/fathinrahman/machinery/v2/brokers/iface"
	"github.com/fathinrahman/machinery/v2/common"
	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/log"
	"github.com/fathinrahman/machinery/v2/tasks"
)

// TaskStatus defines the task status enum
type TaskStatus int

const (
	// Pending task is still waiting to be processed
	TaskStatusPending TaskStatus = iota
	// InProgress task is currently being processed
	TaskStatusInProgress
	// Done task has been completed successfully
	TaskStatusDone
	// Failed task has failed during processing
	TaskStatusFailed
)

const (
	// Lock Constants
	TaskRecoveryLockKey = "task_recovery" // lock key for task recovery process
)

// Broker implements iface.Broker for MongoDB, including native delayed task support.
type Broker struct {
	common.Broker
	common.MongoDBConnector

	// MongoDB
	uri             string
	dbName          string
	timeout         time.Duration
	queue           string
	taskCollName    string
	lockCollName    string
	stuckTaskExpiry time.Duration

	taskColl          *mongo.Collection
	lockColl          *mongo.Collection
	processingWG      sync.WaitGroup
	recoveryWG        sync.WaitGroup
	recoveryLockOwner bool
}

type Task struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	Queue     string             `bson:"queue"`
	Signature tasks.Signature    `bson:"signature"`
	Status    TaskStatus         `bson:"status"`
	CreatedAt time.Time          `bson:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at"`
	ErrorMsg  string             `bson:"errorMsg,omitempty"`
}

// New returns a new MongoDB-backed Machinery broker as iface.Broker.
func New(
	cnf *config.Config,
	queue string,
	dbName string,
	taskCollName string,
	lockCollName string,
	timeout time.Duration,
	stuckTaskExpiry time.Duration,
) iface.Broker {
	return &Broker{
		Broker:          common.NewBroker(cnf),
		taskCollName:    taskCollName,
		lockCollName:    lockCollName,
		dbName:          dbName,
		queue:           queue,
		timeout:         timeout,
		stuckTaskExpiry: stuckTaskExpiry,
		uri:             cnf.Broker,
	}
}

// Publish inserts a new task into MongoDB with "pending" status and handles ETA.
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	now := time.Now().UTC()
	eta := signature.ETA
	if eta == nil || eta.IsZero() {
		signature.ETA = &now
	}

	if signature.AttemptCount == 0 {
		task := Task{
			Queue:     b.queue,
			Signature: *signature,
			Status:    TaskStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if _, err := b.taskColl.InsertOne(ctx, task); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				// Idempotency: Already exists
				log.WARNING.Println("failed to insert: idempotency on UUID")
				return err
			}

			log.ERROR.Println("failed to insert task when publishing", err)
			return err
		}

		return nil
	}

	// Update for retry
	filter := bson.M{"signature.uuid": signature.UUID}
	update := bson.M{
		"$set": bson.M{
			"signature":  signature,
			"status":     TaskStatusPending,
			"updated_at": now,
		},
	}

	opts := options.Update()
	res, err := b.taskColl.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.ERROR.Println("retry failed: failed to update task when publishing", err)

		return err
	}

	if res.MatchedCount == 0 {
		errMsg := fmt.Sprintf("retry failed: task not found with signature UUID: %s", signature.UUID)
		log.ERROR.Printf(errMsg)

		return fmt.Errorf(errMsg)
	}

	return nil
}

// StartConsuming launches N workers that poll MongoDB for tasks as fast as possible.
func (b *Broker) StartConsuming(
	consumerTag string,
	concurrency int,
	processor iface.TaskProcessor,
) (bool, error) {
	log.INFO.Println("StartConsuming: Starting MongoDB broker...")

	if concurrency < 1 {
		concurrency = 1
	}

	// Connect to MongoDB and set collection pointer
	conn, err := b.MongoDBConnector.Connect(b.uri, b.dbName, b.timeout, b.Options)
	if err != nil {
		log.ERROR.Println("Failed to connect to MongoDB:", err)
		b.GetRetryFunc()(b.GetRetryStopChan())
		if b.GetRetry() {
			return b.GetRetry(), err
		}

		return b.GetRetry(), errs.ErrConsumerStopped
	}
	b.MongoDBConnector = *conn
	b.taskColl = conn.Collection(b.taskCollName)
	b.lockColl = conn.Collection(b.lockCollName)

	if err := b.createMongoIndexes(); err != nil {
		return b.GetRetry(), err
	}

	if err := b.initRecoveryLock(); err != nil {
		return b.GetRetry(), err
	}

	// Start embedded broker logic (for retry, stop, etc)
	b.Broker.StartConsuming(consumerTag, concurrency, processor)

	// Worker-pool-based polling: each worker is a polling goroutine
	for i := 0; i < concurrency; i++ {
		b.processingWG.Add(1)
		go func(workerIdx int) {
			defer func() {
				if r := recover(); r != nil {
					log.ERROR.Printf("Worker %d panicked: %v", workerIdx, r)
				} else {
					log.INFO.Printf("Worker %d finished processing", workerIdx)
				}
				b.processingWG.Done()
			}()
			for {
				select {
				case <-b.Broker.GetStopChan():
					return
				default:
					task := b.claimNextTask()
					if task != nil {
						log.DEBUG.Printf("Worker %d claiming task: %s", workerIdx, task.Signature.UUID)
						b.handleTask(processor, &task.Signature)
					} else {
						// No task, avoid db hammering
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}(i)
	}

	// Start recovery task
	b.recoveryWG.Add(1)
	go func() {
		defer b.recoveryWG.Done()

		lockTicker := time.NewTicker(1 * time.Minute)
		defer lockTicker.Stop()

		for {
			select {
			case <-b.Broker.GetStopChan():
				return
			case <-lockTicker.C:
				// Try to acquire the lock and proceed with recovery
				if b.lockRecovery() {
					b.recoveryLockOwner = true
					// Start recovery periodic task with ticker
					recoveryTicker := time.NewTicker(1 * time.Minute)
					defer recoveryTicker.Stop()

					// Recovery loop
					for {
						select {
						case <-b.Broker.GetStopChan():
							return // Stop recovery if stop channel is signaled
						case <-recoveryTicker.C:
							_ = b.recoverStuckTasks(b.stuckTaskExpiry) // Periodically recover stuck tasks
							_ = b.sendRecoveryLockHeartbeat()
						}
					}
				}
			}
		}
	}()

	// Wait for all workers to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// claimNextTask atomically claims the next available eligible task (returns nil if none found).
func (b *Broker) claimNextTask() *Task {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()
	now := time.Now().UTC()
	filter := bson.M{
		"queue":  b.queue,
		"status": TaskStatusPending,
		// claim only registered tasks
		"signature.name": bson.M{
			"$in": b.Broker.GetRegisteredTaskNames(),
		},
		"signature.eta": bson.M{
			"$lte": now,
		},
	}
	update := bson.M{
		"$set": bson.M{
			"status":     TaskStatusInProgress,
			"updated_at": now,
		},
		"$inc": bson.M{
			"signature.attemptcount": 1,
		},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{
			Key:   "signature.eta",
			Value: 1,
		}}).
		SetReturnDocument(options.After)

	var task Task
	err := b.taskColl.FindOneAndUpdate(ctx, filter, update, opts).Decode(&task)
	if err != nil {
		return nil
	}

	return &task
}

// handleTask executes the given task and updates status in MongoDB.
func (b *Broker) handleTask(processor iface.TaskProcessor, signature *tasks.Signature) {
	now := time.Now().UTC()
	set := bson.M{
		"updated_at": now,
	}

	if err := processor.Process(signature); err != nil {
		set["status"] = TaskStatusFailed
		set["errorMsg"] = err.Error()
	} else {
		set["status"] = TaskStatusDone
		set["delete_at"] = now
	}

	filter := bson.M{
		"signature.uuid": signature.UUID,
		"status":         TaskStatusInProgress,
	}

	update := bson.M{
		"$set": set,
	}

	if _, err := b.taskColl.UpdateOne(context.Background(), filter, update); err != nil {
		log.ERROR.Printf("handleTask: Failed to update task status for %s: %v", signature.UUID, err)
	}
}

// recoveryLock tries to acquire a lock for the recovery process
func (b *Broker) lockRecovery() bool {
	now := time.Now().UTC()
	// 3 minutes threshold for stale lock, heartbeat interval is 1 minute
	threshold := now.Add(-3 * time.Minute)

	filter := bson.M{
		"key": TaskRecoveryLockKey,
		"$or": []bson.M{
			{"locked": 0},
			{"updated_at": bson.M{"$lt": threshold}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"locked":     1, // Mark as locked
			"updated_at": now,
		},
	}

	// Specify options to return the document after update
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var lockDoc bson.M
	err := b.lockColl.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&lockDoc)
	if err != nil {
		log.DEBUG.Println("Unable to lock recovery or lock already acquired:", err)
		return false
	}

	log.DEBUG.Printf("=== lockRecovery succeed by this worker ====")

	return true
}

// unlockRecovery sets the locked field back to 0 to release the lock
func (b *Broker) unlockRecovery() {
	if !b.recoveryLockOwner {
		return
	}

	if _, err := b.lockColl.UpdateOne(
		context.Background(),
		bson.M{
			"key": TaskRecoveryLockKey,
		},
		bson.M{
			"$set": bson.M{
				"locked": 0, // reset the lock
			},
		},
	); err != nil {
		log.ERROR.Println("Failed to unlock recovery:", err)
	}
}

func (b *Broker) sendRecoveryLockHeartbeat() error {
	if _, err := b.lockColl.UpdateOne(
		context.Background(),
		bson.M{
			"key":    TaskRecoveryLockKey,
			"locked": 1,
		},
		bson.M{
			"$set": bson.M{"updated_at": time.Now().UTC()},
		},
	); err != nil {
		log.ERROR.Println("Failed to send recovery heartbeat:", err)

		return err
	}

	log.DEBUG.Printf("=== sendRecoveryLockHeartbeat ====")
	return nil
}

// RecoverStuckTasks resets old "in_progress" tasks to "pending" for retry.
func (b *Broker) recoverStuckTasks(expiry time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	now := time.Now().UTC()
	threshold := now.Add(-expiry)
	filter := bson.M{
		"status": TaskStatusInProgress,
		"queue":  b.queue,
		"updated_at": bson.M{
			"$lt": threshold,
		},
	}
	update := bson.M{
		"$set": bson.M{
			"status":        TaskStatusPending,
			"signature.eta": now,
			"updated_at":    now,
		},
	}
	res, err := b.taskColl.UpdateMany(ctx, filter, update)

	log.DEBUG.Printf("=== RecoverStuckTasks finished with total data: %d ====", res.MatchedCount)

	return err
}

// StopConsuming stops all workers and pollers gracefully, disconnects from MongoDB.
func (b *Broker) StopConsuming() {
	log.INFO.Println("StopConsuming: Stopping MongoDB broker...")
	b.Broker.StopConsuming()

	b.processingWG.Wait()
	b.recoveryWG.Wait()

	b.unlockRecovery()

	b.MongoDBConnector.Close(context.Background())
}

// SetCollection is for test/DI: allows injecting a custom collection pointer.
func (b *Broker) SetTaskCollection(coll *mongo.Collection) {
	b.taskColl = coll
}

// GetAllPendingTasks returns all pending tasks in the given queue (admin/debug only).
func (b *Broker) GetAllPendingTasks(queue string) ([]Task, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
	defer cancel()

	filter := bson.M{
		"status": TaskStatusPending,
	}
	if queue != "" {
		filter["queue"] = queue
	}

	cur, err := b.taskColl.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var results []Task
	for cur.Next(ctx) {
		var task Task
		if err := cur.Decode(&task); err != nil {
			continue
		}
		results = append(results, task)
	}

	return results, nil
}

func (b *Broker) initRecoveryLock() error {
	now := time.Now().UTC()
	doc := bson.M{
		"key":        TaskRecoveryLockKey,
		"locked":     0,
		"updated_at": now,
	}

	if _, err := b.lockColl.InsertOne(context.Background(), doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Lock already exists, another worker initialized it. That's fine.
			return nil
		}

		log.ERROR.Println("initRecoveryLock: Failed to insert recovery lock:", err)
		return err
	}

	return nil
}

// createMongoIndexes ensures all indexes are in place for the task collection
func (b *Broker) createMongoIndexes() error {
	_, err := b.taskColl.Indexes().CreateMany(
		context.Background(),
		[]mongo.IndexModel{
			// 1. Unique index on signature.uuid
			{
				Keys:    bson.D{{Key: "signature.uuid", Value: 1}},
				Options: options.Index().SetUnique(true),
			},
			// 2. Compound index for claimNextTask (queue, status, signature.eta)
			{
				Keys: bson.D{
					{Key: "queue", Value: 1},
					{Key: "status", Value: 1},
					{Key: "signature.name", Value: 1},
					{Key: "signature.eta", Value: 1},
				},
			},
			// 3. Compound index for fast status updates
			{
				Keys: bson.D{
					{Key: "signature.uuid", Value: 1},
					{Key: "status", Value: 1},
				},
			},
			// 4. Compound index for stuck task recovery (by updated_at)
			{
				Keys: bson.D{
					{Key: "queue", Value: 1},
					{Key: "status", Value: 1},
					{Key: "updated_at", Value: 1},
				},
			},
			// 5. Optional: TTL index for task cleanup, if we add a delete_at field
			{
				Keys:    bson.D{{Key: "delete_at", Value: 1}},
				Options: options.Index().SetExpireAfterSeconds(0),
			},
		},
	)

	if err == nil {
		_, err = b.lockColl.Indexes().CreateMany(
			context.Background(),
			[]mongo.IndexModel{
				// 1. Unique index on key
				{
					Keys:    bson.D{{Key: "key", Value: 1}},
					Options: options.Index().SetUnique(true),
				},
				// 2. Compound index for claimNextTask (queue, status, signature.eta)
				{
					Keys: bson.D{
						{Key: "key", Value: 1},
						{Key: "locked", Value: 1},
					},
				},
			},
		)
	}

	return err
}
