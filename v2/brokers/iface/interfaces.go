package iface

import (
	"context"
	"reflect"

	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/middlewares"
	"github.com/fathinrahman/machinery/v2/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	GetDelayedTasks() ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
	SetReflectHandler(taskName string, handler func(tasks.Signature) ([]reflect.Value, error))
	SetGlobalMiddleware(globalMiddlewares ...middlewares.MiddlewareFn)
	SetTaskMiddleware(taskName string, middlewares ...middlewares.MiddlewareFn)
	GetReflectHandlers() map[string]func(tasks.Signature) ([]reflect.Value, error)
	GetGlobalMiddlewares() []middlewares.MiddlewareFn
	GetTaskMiddlewares() map[string][]middlewares.MiddlewareFn
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
	PreConsumeHandler() bool
}
