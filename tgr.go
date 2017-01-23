package tgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

var _ error = new(ErrPanic)

type ErrPanic struct {
	rec interface{}
}

func (e ErrPanic) Error() string {
	return fmt.Sprintf("PANIC %#v", e.rec)
}

type Worker func(ctx context.Context) error

func T(w Worker, deps ...*Task) *Task {
	return &Task{
		Worker: w,
		Deps:   deps,
	}
}

func Exec(ctx context.Context, tasks ...*Task) error {
	newCtx, cancel := context.WithCancel(ctx)
	return exec(newCtx, cancel, tasks...)
}

func exec(ctx context.Context, cancel context.CancelFunc, tasks ...*Task) error {
	ntasks := int32(len(tasks))

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if ntasks == 0 {
		return nil
	}

	res := make(chan error, len(tasks))

	for _, t := range tasks {
		go (func(t *Task) {
			var err error
			defer (func() {
				res <- err
				if 0 == atomic.AddInt32(&ntasks, -1) {
					close(res)
				}
			})()
			err = exec(ctx, cancel, t.Deps...)
			if err == nil {
				err = t.run(ctx)
			} else {
				cancel()
			}
		})(t)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err, ok := <-res:
			if ok {
				if err != nil {
					return err
				}
			} else {
				return nil
			}
		}
	}
}

type Task struct {
	Worker Worker
	Deps   []*Task

	done bool
	err  error
	lock sync.Mutex
}

func (t *Task) run(ctx context.Context) (err error) {
	t.lock.Lock()
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
