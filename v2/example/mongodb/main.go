package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/urfave/cli"

	"github.com/RichardKnop/machinery/v2"
	"github.com/RichardKnop/machinery/v2/backends/mongo"
	"github.com/RichardKnop/machinery/v2/backends/result"
	"github.com/RichardKnop/machinery/v2/brokers/mongodb"
	"github.com/RichardKnop/machinery/v2/config"
	exampletasks "github.com/RichardKnop/machinery/v2/example/tasks"
	"github.com/RichardKnop/machinery/v2/example/tracers"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
)

var (
	app *cli.App
)

func init() {
	app = cli.NewApp()
	app.Name = "machinery"
	app.Usage = "machinery worker and send example tasks with machinery send"
	app.Version = "0.0.0"
}

func main() {
	app.Commands = []cli.Command{
		{
			Name:  "worker",
			Usage: "launch machinery worker",
			Action: func(c *cli.Context) error {
				if err := worker(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "send_task",
			Usage: "send a single task",
			Action: func(c *cli.Context) error {
				return sendTask()
			},
		},
		{
			Name:  "send_tasks",
			Usage: "send many tasks",
			Action: func(c *cli.Context) error {
				if err := sendTasks(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "send_concurrency_test",
			Usage: "Run n tasks with a worker of x concurrency to show parallel execution",
			Action: func(c *cli.Context) error {
				return concurrencyTest()
			},
		},
	}
	_ = app.Run(os.Args)
}

func mongoConfig() *config.Config {
	uri := "mongodb://mongo:mongo@localhost:27017/?authSource=admin&readPreference=primary"
	return &config.Config{
		Broker:        uri,
		ResultBackend: uri,
		DefaultQueue:  "test",
		// expire results after 30 min
		ResultsExpireIn: 1800,
	}
}

func startServer() (*machinery.Server, error) {
	cnf := mongoConfig()

	broker := mongodb.New(
		cnf,
		"test",        // queue
		"machinery",   // dbNames
		"task",        // taskCollName
		"lock",        // lockCollName
		3*time.Second, // timeout
		5*time.Minute, // stuck task expiry
	)

	// For unit test/localhost, you might need to set the collection manually
	if mongoBroker, ok := broker.(*mongodb.Broker); ok {
		conn, err := mongoBroker.MongoDBConnector.Connect(cnf.Broker, "machinery", 3*time.Second, mongoBroker.Options)
		if err != nil {
			return nil, fmt.Errorf("MongoDB Connect failed: %v", err)
		}
		mongoBroker.MongoDBConnector = *conn
		mongoBroker.SetTaskCollection(conn.Collection("task"))
	}

	backend, err := mongo.New(cnf, "result", "group_meta")
	if err != nil {
		return nil, fmt.Errorf("mongo.New backend failed: %v", err)
	}
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	// Register example tasks (reuse Redis example tasks)
	tasksMap := map[string]interface{}{
		"add":               exampletasks.Add,
		"multiply":          exampletasks.Multiply,
		"sum_ints":          exampletasks.SumInts,
		"sum_floats":        exampletasks.SumFloats,
		"concat":            exampletasks.Concat,
		"split":             exampletasks.Split,
		"panic_task":        exampletasks.PanicTask,
		"long_running_task": exampletasks.LongRunningTask,
	}
	return server, server.RegisterTasks(tasksMap)
}

func worker() error {
	consumerTag := "machinery_worker"

	cleanup, err := tracers.SetupTracer(consumerTag)
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	worker := server.NewWorker(consumerTag, 1)

	worker.SetPostTaskHandler(func(signature *tasks.Signature) {
		log.INFO.Println("End of task handler for:", signature.Name)
	})
	worker.SetErrorHandler(func(err error) {
		log.ERROR.Println("Error handler:", err)
	})
	worker.SetPreTaskHandler(func(signature *tasks.Signature) {
		log.INFO.Println("Start of task handler for:", signature.Name)
	})

	return worker.Launch()
}

func sendTask() error {
	cleanup, err := tracers.SetupTracer("send-task")
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	now := time.Now()

	// Create task with ETA n seconds from now
	eta := time.Now().Add(10 * time.Second)
	signature := &tasks.Signature{
		// UUID: "idempotency-test-uuid", // Comment this to ignore idempotency
		Name: "add",
		Args: []tasks.Arg{
			{Type: "int64", Value: int64(1)},
			{Type: "int64", Value: int64(2)},
		},
		ETA: &now,
	}
	ctx := context.Background()
	asyncResult, err := server.SendTaskWithContext(ctx, signature)
	if err != nil {
		log.ERROR.Printf("Could not send task: %s\n", err.Error())

		return fmt.Errorf("could not send task: %s", err.Error())
	}

	log.INFO.Printf(
		"Task sent with ETA = %s (wait 10s before worker picks it up), task UUID: %s\n",
		eta.Format(time.RFC3339), asyncResult.Signature.UUID,
	)

	return nil
}

func sendTasks() error {
	cleanup, err := tracers.SetupTracer("sender-tasks")
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	var (
		addTask0, addTask1, addTask2                      tasks.Signature
		multiplyTask0, multiplyTask1                      tasks.Signature
		sumIntsTask, sumFloatsTask, concatTask, splitTask tasks.Signature
		panicTask                                         tasks.Signature
		longRunningTask                                   tasks.Signature
	)

	var initTasks = func() {
		addTask0 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{Type: "int64", Value: int64(1)},
				{Type: "int64", Value: int64(1)},
			},
		}
		addTask1 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{Type: "int64", Value: int64(2)},
				{Type: "int64", Value: int64(2)},
			},
		}
		addTask2 = tasks.Signature{
			Name: "add",
			Args: []tasks.Arg{
				{Type: "int64", Value: int64(5)},
				{Type: "int64", Value: int64(6)}, // make these distinct for demo
			},
		}
		multiplyTask0 = tasks.Signature{
			Name: "multiply",
			Args: []tasks.Arg{
				{Type: "int64", Value: int64(4)},
			},
		}
		multiplyTask1 = tasks.Signature{Name: "multiply"}
		sumIntsTask = tasks.Signature{
			Name: "sum_ints",
			Args: []tasks.Arg{
				{Type: "[]int64", Value: []int64{1, 2}}, // this is correct
			},
		}
		sumFloatsTask = tasks.Signature{
			Name: "sum_floats",
			Args: []tasks.Arg{
				{Type: "[]float64", Value: []float64{1.5, 2.7}},
			},
		}
		concatTask = tasks.Signature{
			Name: "concat",
			Args: []tasks.Arg{
				{Type: "[]string", Value: []string{"foo", "bar"}},
			},
		}
		splitTask = tasks.Signature{
			Name: "split",
			Args: []tasks.Arg{
				{Type: "string", Value: "foo"},
			},
		}
		panicTask = tasks.Signature{Name: "panic_task", RetryCount: 5}
		longRunningTask = tasks.Signature{Name: "long_running_task"}
	}

	span, ctx := opentracing.StartSpanFromContext(context.Background(), "send")
	defer span.Finish()

	batchID := uuid.New().String()
	span.SetBaggageItem("batch.id", batchID)
	span.LogFields(opentracinglog.String("batch.id", batchID))

	log.INFO.Println("Starting batch:", batchID)
	initTasks()

	// Single task
	log.INFO.Println("Single task:")
	asyncResult, err := server.SendTaskWithContext(ctx, &addTask0)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err := asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting task result failed: %s", err.Error())
	}
	log.INFO.Printf("1 + 1 = %v\n", tasks.HumanReadableResults(results))

	// Task with slice arg/ret
	asyncResult, err = server.SendTaskWithContext(ctx, &sumIntsTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err = asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting task result failed: %s", err.Error())
	}
	log.INFO.Printf("sum([1,2]) = %v\n", tasks.HumanReadableResults(results))

	asyncResult, err = server.SendTaskWithContext(ctx, &sumFloatsTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err = asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting task result failed: %s", err.Error())
	}
	log.INFO.Printf("sum([1.5,2.7]) = %v\n", tasks.HumanReadableResults(results))

	asyncResult, err = server.SendTaskWithContext(ctx, &concatTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err = asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting task result failed: %s", err.Error())
	}
	log.INFO.Printf("concat([\"foo\",\"bar\"]) = %v\n", tasks.HumanReadableResults(results))

	asyncResult, err = server.SendTaskWithContext(ctx, &splitTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err = asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting task result failed: %s", err.Error())
	}
	log.INFO.Printf("split([\"foo\"]) = %v\n", tasks.HumanReadableResults(results))

	// Parallel group of tasks
	initTasks()
	log.INFO.Println("Group of tasks (parallel execution):")
	group, err := tasks.NewGroup(&addTask0, &addTask1, &addTask2, &multiplyTask0, &multiplyTask1, &sumFloatsTask, &concatTask, &splitTask)
	if err != nil {
		return fmt.Errorf("error creating group: %s", err.Error())
	}
	asyncResults, err := server.SendGroupWithContext(ctx, group, 10)
	if err != nil {
		return fmt.Errorf("could not send group: %s", err.Error())
	}
	for _, asyncResult := range asyncResults {
		results, err = asyncResult.Get(time.Second * 3)
		if err != nil {
			return fmt.Errorf("getting group task result failed: %s", err.Error())
		}
		log.INFO.Printf(
			"%s: %s\n",
			asyncResult.Signature.Name, tasks.HumanReadableResults(results),
		)
	}

	// Group with callback (Chord)
	initTasks()
	log.INFO.Println("Group of tasks with callback (chord):")
	group, err = tasks.NewGroup(&addTask0, &addTask1, &addTask2)
	if err != nil {
		return fmt.Errorf("error creating group: %s", err)
	}
	chord, err := tasks.NewChord(group, &multiplyTask1)
	if err != nil {
		return fmt.Errorf("error creating chord: %s", err)
	}
	chordAsyncResult, err := server.SendChordWithContext(ctx, chord, 1)
	if err != nil {
		return fmt.Errorf("could not send chord: %s", err.Error())
	}
	results, err = chordAsyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting chord result failed: %s", err.Error())
	}
	log.INFO.Printf("(1 + 1) * (2 + 2) * (5 + 6) = %v\n", tasks.HumanReadableResults(results))

	// Chain of tasks
	initTasks()
	log.INFO.Println("Chain of tasks:")
	chain, err := tasks.NewChain(&addTask0, &addTask1, &addTask2, &multiplyTask0)
	if err != nil {
		return fmt.Errorf("error creating chain: %s", err)
	}
	chainAsyncResult, err := server.SendChainWithContext(ctx, chain)
	if err != nil {
		return fmt.Errorf("could not send chain: %s", err.Error())
	}
	results, err = chainAsyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting chain result failed: %s", err.Error())
	}
	log.INFO.Printf("(((1 + 1) + (2 + 2)) + (5 + 6)) * 4 = %v\n", tasks.HumanReadableResults(results))

	// Panic task
	initTasks()
	asyncResult, err = server.SendTaskWithContext(ctx, &panicTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	_, err = asyncResult.Get(time.Second * 3)
	if err == nil {
		return errors.New("error should not be nil if task panicked")
	}
	log.INFO.Printf("Task panicked and returned error = %v\n", err.Error())

	// Long running task
	initTasks()
	asyncResult, err = server.SendTaskWithContext(ctx, &longRunningTask)
	if err != nil {
		return fmt.Errorf("could not send task: %s", err.Error())
	}
	results, err = asyncResult.Get(time.Second * 3)
	if err != nil {
		return fmt.Errorf("getting long running task result failed: %s", err.Error())
	}
	log.INFO.Printf("Long running task returned = %v\n", tasks.HumanReadableResults(results))

	return nil
}

func concurrencyTest() error {
	cleanup, err := tracers.SetupTracer("send-concurrency-test")
	if err != nil {
		log.FATAL.Fatalln("Unable to instantiate a tracer:", err)
	}
	defer cleanup()

	server, err := startServer()
	if err != nil {
		return err
	}

	// 3 tasks with sleeps to make concurrency visible
	sleepTask := func() *tasks.Signature {
		return &tasks.Signature{
			Name: "long_running_task",
			// No ETA, immediate
		}
	}

	fmt.Println("Submitting 3 long running tasks...")

	// Send 3 tasks
	var results []*result.AsyncResult
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		asyncResult, err := server.SendTaskWithContext(ctx, sleepTask())
		if err != nil {
			return fmt.Errorf("could not send task #%d: %w", i, err)
		}
		results = append(results, asyncResult)
		fmt.Printf("Task %d sent: UUID=%s\n", i, asyncResult.Signature.UUID)
	}

	fmt.Println("\nTo test concurrency, edit your worker() to: worker := server.NewWorker(consumerTag, 3) then run it")

	fmt.Println("Waiting for results...")
	for i, asyncResult := range results {
		_, err := asyncResult.Get(15 * time.Second)
		if err != nil {
			fmt.Printf("Task %d with UUID: %s error: %v\n", i+1, asyncResult.Signature.UUID, err)
		} else {
			fmt.Printf("Task %d with UUID: %s finished.\n", i+1, asyncResult.Signature.UUID)
		}
	}
	fmt.Println("All done!")
	return nil
}
