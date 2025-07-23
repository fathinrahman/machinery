// v2/middlewares/middleware.go

package middlewares

import (
	"context"
	"reflect"
)

// ExecuteHandlerFn defines the handler function signature
type ExecuteHandlerFn func(
	ctx context.Context,
	taskName string,
	ID string,
	headers map[string]any,
	args []reflect.Value) error

// type ExecuteHandlerFn func(ctx context.Context, params ...interface{}) (interface{}, error)

// MiddlewareFn defines the type of middleware function
type MiddlewareFn func(next ExecuteHandlerFn) ExecuteHandlerFn

// ApplyMiddleware applies a series of middlewares to the given handler
func ApplyMiddleware(h ExecuteHandlerFn, middleware ...MiddlewareFn) ExecuteHandlerFn {
	for i := len(middleware) - 1; i >= 0; i-- {
		h = middleware[i](h)
	}
	return h
}
