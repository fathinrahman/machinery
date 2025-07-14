package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

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

// Broker implements iface.Broker for MongoDB, including native delayed task support.
type Broker struct {
	common.Broker
	common.MongoDBConnector

	// Configuration
	queue           string
	stuckTaskExpiry time.Duration

	// MongoDB collection
	taskColl *mongo.Collection
	lockColl *mongo.Collection

	// Sync wait groups for processing task and recovering stuck tasks
	processingWG sync.WaitGroup
	recoveryWG   sync.WaitGroup
}

type Task struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Queue     string             `bson:"queue" json:"queue"`
	Signature tasks.Signature    `bson:"signature" json:"signature"`
	Status    TaskStatus         `bson:"status" json:"status"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
	UpdatedAt time.Time          `bson:"updated_at" json:"updated_at"`
	ErrorMsg  string             `bson:"errorMsg,omitempty" json:"errorMsg,omitempty"`
}

// New returns a new MongoDB-backed Machinery broker as iface.Broker.
func New(
	cnf *config.Config,
	queue string,
	taskCollName string,
	lockCollName string,
	stuckTaskExpiry time.Duration,
) (iface.Broker, error) {
	if err := validateBrokerParams(cnf, queue, taskCollName, lockCollName); err != nil {
		return nil, err
	}

	b := &Broker{
		queue:           queue,
		stuckTaskExpiry: stuckTaskExpiry,
	}

	client, db, err := b.MongoDBConnector.Connect(cnf)
	if err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %v", err)
	}

	cnf.MongoDB.Client = client

	b.Broker = common.NewBroker(cnf)

	b.taskColl = db.Collection(taskCollName)
	b.lockColl = db.Collection(lockCollName)

	return b, nil
}

func validateBrokerParams(
	cnf *config.Config,
	queue string,
	taskCollName string,
	lockCollName string,
) error {
	if cnf == nil || cnf.Broker == "" || cnf.MongoDB == nil {
		return errors.New("MongoDB configuration is required")
	}
	if queue == "" {
		return errors.New("queue must be provided")
	}
	if taskCollName == "" || lockCollName == "" {
		return errors.New("task and lock collection names must be provided")
	}

	return nil
}

// Publish inserts a new task into MongoDB with "pending" status and handles ETA.
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	now := time.Now().UTC()
	eta := signature.ETA
	if eta == nil || eta.IsZero() {
		signature.ETA = &now
	} else if signature.ETA.Location() != time.UTC {
		*signature.ETA = signature.ETA.UTC()
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
				log.DEBUG.Println("publish failed to insert: idempotency on UUID")
				return err
			}
			log.ERROR.Println("publish failed to insert:", err)

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
		log.ERROR.Printf("publish retry failed to update: error with signature UUID %s: %v", signature.UUID, err)

		return err
	}

	if res.MatchedCount == 0 {
		errMsg := fmt.Sprintf("publish retry failed to update: task not found with signature UUID: %s", signature.UUID)
		log.ERROR.Println(errMsg)

		return errors.New(errMsg)
	}

	return nil
}

// StartConsuming launches N workers that poll MongoDB for tasks as fast as possible.
func (b *Broker) StartConsuming(
	consumerTag string,
	concurrency int,
	processor iface.TaskProcessor,
) (bool, error) {
	log.INFO.Printf("***** start consuming: MongoDB broker on queue '%s' with concurrency %d *****", b.queue, concurrency)

	if concurrency < 1 {
		concurrency = 1
	}

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
					log.ERROR.Println("Worker %d panicked: %v", workerIdx, r)
				} else {
					log.INFO.Println("Worker %d finished processing", workerIdx)
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

		// Calculate how long to wait until the next full minute
		// So recovery runs at the beginning of each minute
		now := time.Now().UTC()
		nextMinute := now.Truncate(time.Minute).Add(time.Minute)
		for {
			wait := time.Until(nextMinute)

			select {
			case <-b.Broker.GetStopChan():
				return
			case <-time.After(wait):
				if b.lockRecovery() {
					b.recoverStuckTasks(b.stuckTaskExpiry)
					b.unlockRecovery()
				}
				nextMinute = nextMinute.Add(time.Minute) // Move to the next minute
			}
		}
	}()

	// Wait for all workers to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// claimNextTask atomically claims the next available eligible task (returns nil if none found).
func (b *Broker) claimNextTask() *Task {
	registeredTaskNames := b.Broker.GetRegisteredTaskNames()
	if len(registeredTaskNames) == 0 {
		return nil
	}

	now := time.Now().UTC()
	filter := bson.M{
		"queue":  b.queue,
		"status": TaskStatusPending,
		// claim only registered tasks
		"signature.name": bson.M{
			"$in": registeredTaskNames,
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
	err := b.taskColl.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&task)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			log.ERROR.Println("claim next task failed with error:", err)
		}

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
		log.ERROR.Printf("handle task failed to update: error with signature UUID %s: %v", signature.UUID, err)
	}
}

// StopConsuming stops all workers and pollers gracefully, disconnects from MongoDB.
func (b *Broker) StopConsuming() {
	log.INFO.Printf("***** stop consuming: MongoDB broker on queue '%s' *****", b.queue)
	b.Broker.StopConsuming()

	b.processingWG.Wait()
	b.recoveryWG.Wait()

	b.MongoDBConnector.Close(context.Background(), b.GetConfig().MongoDB.Client)
}

type lockStatus int

const (
	// lockStatusUnlocked indicates the lock is available for acquisition
	lockStatusUnlocked lockStatus = iota
	// lockStatusLocked indicates the lock is currently held by a worker
	lockStatusLocked
)

const (
	taskRecoveryLockKey = "task_recovery"  // lock key for task recovery process
	staleLockThreshold  = -5 * time.Minute // 5 minutes threshold for stale lock
)

type lock struct {
	Key          string     `bson:"key"`
	Status       lockStatus `bson:"status"` // 0 for unlocked, 1 for locked
	LastLockedAt time.Time  `bson:"last_locked_at"`
}

func (b *Broker) initRecoveryLock() error {
	now := time.Now().UTC()
	lock := lock{
		Key:          taskRecoveryLockKey,
		Status:       lockStatusUnlocked,
		LastLockedAt: now,
	}

	if _, err := b.lockColl.InsertOne(context.Background(), lock); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// Lock already exists, another worker initialized it.
			return nil
		}

		log.ERROR.Println("init recovery lock failed to insert:", err)
		return err
	}

	return nil
}

// recoveryLock tries to acquire a lock for the recovery process
func (b *Broker) lockRecovery() bool {
	now := time.Now().UTC()
	staleThreshold := now.Add(staleLockThreshold) // 5 minutes stale threshold

	filter := bson.M{
		"key": taskRecoveryLockKey,
		"$or": []bson.M{
			{"status": lockStatusUnlocked},
			// handle if worker crashed and left the lock in a locked state
			{"status": lockStatusLocked, "last_locked_at": bson.M{"$lte": staleThreshold}},
		},
	}

	update := bson.M{
		"$set": bson.M{
			"status":         lockStatusLocked,
			"last_locked_at": now,
		},
	}

	// specify options to return the document after update
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)

	var lockDoc lock
	err := b.lockColl.FindOneAndUpdate(context.Background(), filter, update, opts).Decode(&lockDoc)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			log.ERROR.Println("lock recovery failed to find one and update:", err)
		}

		return false
	}

	log.DEBUG.Println("lock recovery succeed by this worker")

	return true
}

// unlockRecovery sets the locked field back to 0 to release the lock
func (b *Broker) unlockRecovery() {
	filter := bson.M{"key": taskRecoveryLockKey}
	update := bson.M{"$set": bson.M{"status": lockStatusUnlocked}}
	if _, err := b.lockColl.UpdateOne(context.Background(), filter, update); err != nil {
		log.ERROR.Println("unlock recovery failed to update:", err)
	}
}

// recoverStuckTasks resets old "in_progress" tasks to "pending" for retry.
func (b *Broker) recoverStuckTasks(expiry time.Duration) {
	if expiry <= 0 {
		expiry = 15 * time.Minute // default expiry if not set
	}
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
	res, err := b.taskColl.UpdateMany(context.Background(), filter, update)
	if err != nil {
		log.ERROR.Printf("recover stuck tasks failed to update:", err)

		return
	}

	log.DEBUG.Printf("recover stuck tasks finished with total data: %d", res.MatchedCount)
}

// createMongoIndexes ensures all indexes are in place for the task collection
func (b *Broker) createMongoIndexes() error {
	if _, err := b.taskColl.Indexes().CreateMany(
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
	); err != nil {
		log.ERROR.Println("create mongo indexes failed on task collection:", err)
	}

	if _, err := b.lockColl.Indexes().CreateMany(
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
					{Key: "status", Value: 1},
					{Key: "last_locked_at", Value: 1},
				},
			},
		},
	); err != nil {
		log.ERROR.Println("create mongo indexes failed on lock collection:", err)
	}

	return nil
}

// GetAllTasksByStatusWithLimit returns all tasks by status with limit (admin/debug only).
func (b *Broker) GetAllTasksByStatusWithLimit(status TaskStatus, limit int64) ([]Task, error) {
	ctx := context.Background()
	filter := bson.M{
		"queue":  b.queue,
		"status": status,
	}

	if limit <= 0 {
		limit = 10 // default limit if not specified
	}

	opts := options.Find().SetLimit(limit)
	cur, err := b.taskColl.Find(ctx, filter, opts)
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
