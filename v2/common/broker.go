package common

import (
	"errors"
	"reflect"
	"sync"

	"github.com/fathinrahman/machinery/v2/brokers/iface"
	"github.com/fathinrahman/machinery/v2/config"
	"github.com/fathinrahman/machinery/v2/log"
	"github.com/fathinrahman/machinery/v2/middlewares"
	"github.com/fathinrahman/machinery/v2/retry"
	"github.com/fathinrahman/machinery/v2/tasks"
)

type registeredTaskNames struct {
	sync.RWMutex
	items []string
}

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames registeredTaskNames
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
	reflectHandlers     map[string]func(tasks.Signature) ([]reflect.Value, error)
	middlewares         []middlewares.MiddlewareFn
	taskMiddlewares     map[string][]middlewares.MiddlewareFn
}

// NewBroker creates new Broker instance
func NewBroker(cnf *config.Config) Broker {
	return Broker{
		cnf:             cnf,
		retry:           true,
		stopChan:        make(chan int),
		retryStopChan:   make(chan int),
		reflectHandlers: make(map[string]func(tasks.Signature) ([]reflect.Value, error)),
		middlewares:     []middlewares.MiddlewareFn{},
		taskMiddlewares: make(map[string][]middlewares.MiddlewareFn),
	}
}

// GetConfig returns config
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

// GetRetry ...
func (b *Broker) GetRetry() bool {
	return b.retry
}

// GetRetryFunc ...
func (b *Broker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan ...
func (b *Broker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan ...
func (b *Broker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// SetRegisteredTaskNames sets registered task names
func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *Broker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *Broker) StopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		log.WARNING.Print("Stopping retry closure.")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	log.WARNING.Print("Stop channel")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *Broker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}

func (b *Broker) SetReflectHandler(taskName string, fn func(tasks.Signature) ([]reflect.Value, error)) {
	if b.reflectHandlers == nil {
		b.reflectHandlers = make(map[string]func(tasks.Signature) ([]reflect.Value, error))
	}

	b.reflectHandlers[taskName] = fn
}

// SetGlobalMiddleware sets the global middlewares
func (b *Broker) SetGlobalMiddleware(middlewares ...middlewares.MiddlewareFn) {
	b.middlewares = middlewares
}

// SetTaskMiddleware sets the task-specific middlewares
func (b *Broker) SetTaskMiddleware(taskName string, middlewares ...middlewares.MiddlewareFn) {
	b.taskMiddlewares[taskName] = middlewares
}

func (b *Broker) GetReflectHandlers() map[string]func(tasks.Signature) ([]reflect.Value, error) {
	return b.reflectHandlers
}

func (b *Broker) GetGlobalMiddlewares() []middlewares.MiddlewareFn {
	return b.middlewares
}

func (b *Broker) GetTaskMiddlewares() map[string][]middlewares.MiddlewareFn {
	return b.taskMiddlewares
}

// ApplyMiddlewares applies the middlewares to a given handler
func (b *Broker) ApplyMiddlewares(handler middlewares.ExecuteHandlerFn, taskName string) middlewares.ExecuteHandlerFn {
	// Apply global middlewares first
	handler = middlewares.ApplyMiddleware(handler, b.middlewares...)

	// Apply task-specific middlewares
	if taskMws, ok := b.taskMiddlewares[taskName]; ok {
		handler = middlewares.ApplyMiddleware(handler, taskMws...)
	}

	return handler
}
