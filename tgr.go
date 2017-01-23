package tgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

var _ error = new(ErrPanic)

// If some task panics returned error will be of this type
type ErrPanic struct {
	rec interface{}
}

func (e ErrPanic) Error() string {
	return fmt.Sprintf("PANIC %#v", e.rec)
}

type Worker func(ctx context.Context) error

// Creates a task
func T(w Worker, deps ...*Task) *Task {
	return &Task{
		Worker: w,
		Deps:   deps,
	}
}

// Executes given tasks in maximum concurrent manner walking recursively
// through the dependency graph.
// If some task returns an error, whole graph execution is being cancelled
// returning error received from first failed task.
func Exec(ctx context.Context, tasks ...*Task) (err error) {
	newCtx, interrupt := context.WithCancel(ctx)
	var lock sync.Mutex
	exec(newCtx, func(e error) {
		lock.Lock()
		defer lock.Unlock()

		if e != nil {
			if err == nil {
				err = e
				interrupt()
			}
		}
	}, tasks...)
	return
}

func exec(ctx context.Context, yieldError func(error), tasks ...*Task) bool {
	ntasks := int32(len(tasks))

	if ntasks == 0 {
		return true
	}

	res := make(chan error, len(tasks))

	for _, t := range tasks {
		go (func(t *Task) {
			defer (func() {
				if 0 == atomic.AddInt32(&ntasks, -1) {
					close(res)
				}
			})()

			if exec(ctx, yieldError, t.Deps...) {
				res <- t.run(ctx)
			}
		})(t)
	}

	for {
		select {
		case err, ok := <-res:
			if ok {
				if err != nil {
					yieldError(err)
					return false
				}
			} else {
				return true
			}
		case <-ctx.Done():
			yieldError(ctx.Err())
			return false
		}
	}
}

// Represents single task in graph
type Task struct {
	Worker Worker
	Deps   []*Task

	done bool
	err  error
	lock sync.Mutex
}

func (t *Task) run(ctx context.Context) (err error) {
	t.lock.Lock() // To prevent worker from being executed multiple times
	defer t.lock.Unlock()

	defer (func() {
		err = t.err
	})()

	if t.done {
		return
	}

	defer func() {
		t.done = true
		if rec := recover(); rec != nil {
			t.err = ErrPanic{rec}
		}
	}()

	t.err = t.Worker(ctx)
	return
}
