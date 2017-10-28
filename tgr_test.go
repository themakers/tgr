package tgr

import (
	"errors"
	"testing"
	"time"

	"context"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTgr(t *testing.T) {
	Convey("Testing TaskGraph", t, func() {
		errSim := errors.New("Simulated error")
		ctx := context.Background()

		var flow []string

		Tsk := func(id string, tm int, err interface{}, deps ...*Task) *Task {
			line := func(ln string) {
				flow = append(flow, ln)
			}
			return T(func(ctx context.Context) error {
				time.Sleep(time.Duration(tm) * time.Millisecond)
				line(id)
				switch err := err.(type) {
				case nil:
					return nil
				case error:
					return err
				default:
					panic(err)
				}
			}, deps...)
		}

		Convey("Normal flow", func() {

			Convey("Empty", func() {
				So(Exec(ctx), ShouldBeNil)
			})

			Convey("Single task", func() {
				a := Tsk("a", 0, nil)

				So(Exec(ctx, a), ShouldBeNil)
				So(flow, ShouldResemble, []string{
					"a",
				})
			})

			Convey("Single dep", func() {
				a := Tsk("a", 0, nil)
				b := Tsk("b", 0, nil, a)

				So(Exec(ctx, b), ShouldBeNil)
				So(flow, ShouldResemble, []string{"a", "b"})
			})

			Convey("Two deps", func() {
				a := Tsk("a", 0, nil)
				b := Tsk("b", 1, nil)
				c := Tsk("c", 0, nil, a, b)

				So(Exec(ctx, c), ShouldBeNil)
				So(flow, ShouldResemble, []string{"a", "b", "c"})
			})

			Convey("Rhombus", func() {
				a := Tsk("a", 0, nil)
				b := Tsk("b", 0, nil, a)
				c := Tsk("c", 1, nil, a)
				d := Tsk("d", 0, nil, b, c)

				So(Exec(ctx, d), ShouldBeNil)
				So(flow, ShouldResemble, []string{"a", "b", "c", "d"})
			})

			Convey("Multiple entry tasks 1", func() {
				a := Tsk("a", 0, nil)
				b := Tsk("b", 0, nil, a)
				c := Tsk("c", 1, nil, a)

				So(Exec(ctx, b, c), ShouldBeNil)
				So(flow, ShouldResemble, []string{"a", "b", "c"})
			})

			Convey("Multiple entry tasks 2", func() {
				a := Tsk("a", 0, nil)
				b := Tsk("b", 0, nil, a)
				c := Tsk("c", 1, nil, a)
				d := Tsk("d", 1, nil, b, c)
				e := Tsk("e", 0, nil, c)

				So(Exec(ctx, d, e), ShouldBeNil)
				So(flow, ShouldResemble, []string{"a", "b", "c", "e", "d"})
			})

			Convey("Adenine", func() {
				_0 := Tsk("0", 1, nil)
				_6 := Tsk("6", 0, nil, _0)
				_1 := Tsk("1", 0, nil, _6)
				_2 := Tsk("2", 0, nil, _1)
				_7 := Tsk("7", 0, nil)
				_5 := Tsk("5", 1, nil, _6, _7)
				_8 := Tsk("8", 0, nil, _7)
				_4 := Tsk("4", 0, nil, _5)

				_3 := Tsk("3", 1, nil, _4, _2)
				_9 := Tsk("9", 0, nil, _8, _4)

				Convey("Full", func() {
					So(Exec(ctx, _3, _9), ShouldBeNil)
					So(flow, ShouldResemble, []string{
						"7", "8",
						"0", "6",
						"1", "2",
						"5", "4",
						"9", "3",
					})
				})
				Convey("Partial Right", func() {
					So(Exec(ctx, _3), ShouldBeNil)
					So(flow, ShouldResemble, []string{
						"7",
						"0", "6",
						"1", "2",
						"5", "4",
						"3",
					})
				})
				Convey("Partial Left", func() {
					So(Exec(ctx, _9), ShouldBeNil)
					So(flow, ShouldResemble, []string{
						"7", "8",
						"0", "6",
						"5", "4",
						"9",
					})
				})
			})

		})

		Convey("Errors", func() {

			Convey("Reported", func() {

				Convey("Simple", func() {
					a := Tsk("a", 0, errSim)

					So(Exec(ctx, a), ShouldEqual, errSim)
				})

				Convey("Complex", func() {
					a := Tsk("a", 0, nil)
					b := Tsk("b", 0, errSim, a)
					c := Tsk("c", 1, nil, a)
					d := Tsk("d", 1, nil, b, c)
					e := Tsk("e", 0, nil, c)

					So(Exec(ctx, d, e), ShouldEqual, errSim)
				})

			})

			Convey("Panic", func() {

				Convey("Panic", func() {
					a := Tsk("a", 0, "panic")

					So(Exec(ctx, a), ShouldResemble, ErrPanic{"panic"})
					So(Exec(ctx, a).Error(), ShouldResemble, ErrPanic{"panic"}.Error())
				})

				Convey("Panic Complex", func() {
					a := Tsk("a", 0, nil)
					b := Tsk("b", 0, "panic", a)
					c := Tsk("c", 1, nil, a)
					d := Tsk("d", 1, nil, b, c)
					e := Tsk("e", 0, nil, c)

					So(Exec(ctx, d, e), ShouldResemble, ErrPanic{"panic"})
				})

			})

			Convey("Cycle", func() {

				Convey("Cycle 1", func() {
					var b *Task
					a := Tsk("a", 0, nil, b)
					b = Tsk("b", 0, nil, a)

					So(Exec(ctx, a, b), ShouldEqual, errSim)
				})

				Convey("Cycle 2", func() {
					var b *Task
					d := Tsk("b", 0, nil, b)
					c := Tsk("b", 0, nil, d)
					b = Tsk("b", 0, nil, c)
					a := Tsk("a", 0, nil, b)

					So(Exec(ctx, a), ShouldEqual, "wrong value")
				})

			})

		})

		Convey("Context", func() {

			Convey("Deadline exceeded", func() {
				a := T(func(ctx context.Context) error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				})

				ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(10*time.Millisecond))

				So(Exec(ctx, a), ShouldResemble, context.DeadlineExceeded)
			})

			Convey("Deadline NOT exceeded", func() {
				a := T(func(ctx context.Context) error {
					return nil
				})

				ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(10*time.Millisecond))

				So(Exec(ctx, a), ShouldBeNil)
			})

			Convey("Cancellation", func() {
				a := T(func(ctx context.Context) error {
					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				})

				ctx, cancel := context.WithCancel(context.Background())

				go (func() {
					time.Sleep(10 * time.Millisecond)
					cancel()
				})()

				So(Exec(ctx, a), ShouldEqual, context.Canceled)
			})

		})

		Convey("Regression", func() {

			Convey("Such cases was running successfully without error", func() {
				a := Tsk("a", 0, false)
				So(Exec(ctx, a), ShouldResemble, ErrPanic{false})
			})

		})

		Convey("Examples", func() {

			Convey("Task with arguments", func() {
				arg := errSim

				a := T((func(arg error) Worker {
					return func(ctx context.Context) error {
						return arg
					}
				})(arg))

				So(Exec(ctx, a), ShouldEqual, errSim)
			})

		})
	})
}
