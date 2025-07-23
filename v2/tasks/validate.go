package tasks

import (
	"errors"
	"reflect"
)

var (
	// ErrTaskMustBeFunc ...
	ErrTaskMustBeFunc = errors.New("Task must be a func type")
	// ErrTaskReturnsNoValue ...
	ErrTaskReturnsNoValue = errors.New("Task must return at least a single value")
	// ErrLastReturnValueMustBeError ..
	ErrLastReturnValueMustBeError = errors.New("Last return value of a task must be error")

	ErrHandlerMustBeFunc            = errors.New("handler must be a function")
	ErrHandlerReturnsNoValue        = errors.New("handler must return at least one value")
	ErrHandlerLastReturnMustBeError = errors.New("last return value must be of type error")
)

// ValidateTask validates task function using reflection and makes sure
// it has a proper signature. Functions used as tasks must return at least a
// single value and the last return type must be error
func ValidateTask(task interface{}) error {
	v := reflect.ValueOf(task)
	t := v.Type()

	// Task must be a function
	if t.Kind() != reflect.Func {
		return ErrTaskMustBeFunc
	}

	// Task must return at least a single value
	if t.NumOut() < 1 {
		return ErrTaskReturnsNoValue
	}

	// Last return value must be error
	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrLastReturnValueMustBeError
	}

	return nil
}

// ValidateReflectHandler validates a handler function for use in SetReflectHandler
func ValidateReflectHandler(handler interface{}) error {
	v := reflect.ValueOf(handler)
	t := v.Type()

	// Handler must be a function
	if t.Kind() != reflect.Func {
		return ErrHandlerMustBeFunc
	}

	// Handler must return at least one value
	if t.NumOut() < 1 {
		return ErrHandlerReturnsNoValue
	}

	// Last return value must be of type error
	lastReturnType := t.Out(t.NumOut() - 1)
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	if !lastReturnType.Implements(errorInterface) {
		return ErrHandlerLastReturnMustBeError
	}

	return nil
}
