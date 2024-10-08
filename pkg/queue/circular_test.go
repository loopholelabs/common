// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type P struct {
	Int    int
	String string
}

func TestCircular(t *testing.T) {
	t.Parallel()

	testPacket := func() *P {
		return new(P)
	}
	testPacket2 := func() *P {
		p := new(P)
		p.Int = 1
		p.String = "2"
		return p
	}

	t.Run("success", func(t *testing.T) {
		rb := NewCircular[P, *P](1)
		p := testPacket()
		err := rb.Push(p)
		assert.NoError(t, err)
		actual, err := rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p, actual)
	})
	t.Run("out of capacity", func(t *testing.T) {
		rb := NewCircular[P, *P](0)
		err := rb.Push(testPacket())
		assert.NoError(t, err)
	})
	t.Run("out of capacity with non zero capacity, blocking", func(t *testing.T) {
		rb := NewCircular[P, *P](1)
		p1 := testPacket()
		err := rb.Push(p1)
		assert.NoError(t, err)
		doneCh := make(chan struct{}, 1)
		p2 := testPacket2()
		go func() {
			err = rb.Push(p2)
			assert.NoError(t, err)
			doneCh <- struct{}{}
		}()
		select {
		case <-doneCh:
			t.Fatal("LockFree did not block on full write")
		case <-time.After(time.Millisecond * 10):
			actual, err := rb.Pop()
			require.NoError(t, err)
			assert.Equal(t, p1, actual)
			select {
			case <-doneCh:
				actual, err := rb.Pop()
				require.NoError(t, err)
				assert.Equal(t, p2, actual)
			case <-time.After(time.Millisecond * 10):
				t.Fatal("Circular did not unblock on read from full write")
			}
		}
	})
	t.Run("length calculations", func(t *testing.T) {
		rb := NewCircular[P, *P](1)
		p1 := testPacket()

		err := rb.Push(p1)
		assert.NoError(t, err)
		assert.Equal(t, 1, rb.Length())
		assert.Equal(t, uint64(0), rb.head)
		assert.Equal(t, uint64(1), rb.tail)

		actual, err := rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p1, actual)
		assert.Equal(t, 0, rb.Length())
		assert.Equal(t, uint64(1), rb.head)
		assert.Equal(t, uint64(1), rb.tail)

		err = rb.Push(p1)
		assert.NoError(t, err)
		assert.Equal(t, 1, rb.Length())
		assert.Equal(t, uint64(1), rb.head)
		assert.Equal(t, uint64(0), rb.tail)

		rb = NewCircular[P, *P](4)

		err = rb.Push(p1)
		assert.NoError(t, err)
		assert.Equal(t, 1, rb.Length())
		assert.Equal(t, uint64(0), rb.head)
		assert.Equal(t, uint64(1), rb.tail)

		p2 := testPacket2()
		err = rb.Push(p2)
		assert.NoError(t, err)
		assert.Equal(t, 2, rb.Length())
		assert.Equal(t, uint64(0), rb.head)
		assert.Equal(t, uint64(2), rb.tail)

		err = rb.Push(p2)
		assert.NoError(t, err)
		assert.Equal(t, 3, rb.Length())
		assert.Equal(t, uint64(0), rb.head)
		assert.Equal(t, uint64(3), rb.tail)

		actual, err = rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p1, actual)
		assert.Equal(t, 2, rb.Length())
		assert.Equal(t, uint64(1), rb.head)
		assert.Equal(t, uint64(3), rb.tail)

		actual, err = rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p2, actual)
		assert.Equal(t, 1, rb.Length())
		assert.Equal(t, uint64(2), rb.head)
		assert.Equal(t, uint64(3), rb.tail)

		err = rb.Push(p2)
		assert.NoError(t, err)
		assert.Equal(t, 2, rb.Length())
		assert.Equal(t, uint64(2), rb.head)
		assert.Equal(t, uint64(4), rb.tail)

		err = rb.Push(p2)
		assert.NoError(t, err)
		assert.Equal(t, 3, rb.Length())
		assert.Equal(t, uint64(2), rb.head)
		assert.Equal(t, uint64(5), rb.tail)

		actual, err = rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p2, actual)
		assert.Equal(t, 2, rb.Length())
		assert.Equal(t, uint64(3), rb.head)
		assert.Equal(t, uint64(5), rb.tail)

		actual, err = rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p2, actual)
		assert.Equal(t, 1, rb.Length())
		assert.Equal(t, uint64(4), rb.head)
		assert.Equal(t, uint64(5), rb.tail)

		actual, err = rb.Pop()
		require.NoError(t, err)
		assert.Equal(t, p2, actual)
		assert.Equal(t, 0, rb.Length())
		assert.Equal(t, uint64(5), rb.head)
		assert.Equal(t, uint64(5), rb.tail)
	})
	t.Run("buffer closed", func(t *testing.T) {
		rb := NewCircular[P, *P](1)
		assert.False(t, rb.IsClosed())
		rb.Close()
		assert.True(t, rb.IsClosed())
		err := rb.Push(testPacket())
		assert.ErrorIs(t, Closed, err)
		_, err = rb.Pop()
		assert.ErrorIs(t, Closed, err)
	})
	t.Run("pop empty", func(t *testing.T) {
		done := make(chan struct{}, 1)
		rb := NewCircular[P, *P](1)
		go func() {
			_, _ = rb.Pop()
			done <- struct{}{}
		}()
		assert.Equal(t, 0, len(done))
		_ = rb.Push(testPacket())
		<-done
		assert.Equal(t, 0, rb.Length())
	})
	t.Run("partial overflow, blocking", func(t *testing.T) {
		rb := NewCircular[P, *P](4)
		p1 := testPacket()
		p1.Int = 1

		p2 := testPacket()
		p2.Int = 2

		p3 := testPacket()
		p3.Int = 3

		p4 := testPacket()
		p4.Int = 4

		p5 := testPacket()
		p5.Int = 5

		err := rb.Push(p1)
		assert.NoError(t, err)
		err = rb.Push(p2)
		assert.NoError(t, err)
		err = rb.Push(p3)
		assert.NoError(t, err)

		assert.Equal(t, 3, rb.Length())

		actual, err := rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p1, actual)
		assert.Equal(t, 2, rb.Length())

		err = rb.Push(p4)
		assert.NoError(t, err)
		err = rb.Push(p5)
		assert.NoError(t, err)

		assert.Equal(t, 4, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p2, actual)

		assert.Equal(t, 3, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p3, actual)

		assert.Equal(t, 2, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p4, actual)

		assert.Equal(t, 1, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p5, actual)
		assert.NotEqual(t, p1, p5)
		assert.Equal(t, 0, rb.Length())
	})
	t.Run("partial overflow, non-blocking", func(t *testing.T) {
		rb := NewCircular[P, *P](4)
		p1 := testPacket()
		p1.Int = 1

		p2 := testPacket()
		p2.Int = 2

		p3 := testPacket()
		p3.Int = 3

		p4 := testPacket()
		p4.Int = 4

		p5 := testPacket()
		p5.Int = 5

		p6 := testPacket()
		p6.Int = 6

		err := rb.Push(p1)
		assert.NoError(t, err)
		err = rb.Push(p2)
		assert.NoError(t, err)
		err = rb.Push(p3)
		assert.NoError(t, err)
		err = rb.Push(p4)
		assert.NoError(t, err)

		assert.Equal(t, 4, rb.Length())

		err = rb.Push(p5)
		assert.NoError(t, err)

		assert.Equal(t, 5, rb.Length())

		err = rb.Push(p6)
		assert.NoError(t, err)

		assert.Equal(t, 6, rb.Length())

		actual, err := rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p1, actual)

		assert.Equal(t, 5, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p2, actual)

		assert.Equal(t, 4, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p3, actual)

		assert.Equal(t, 3, rb.Length())

		actual, err = rb.Pop()
		assert.NoError(t, err)
		assert.Equal(t, p4, actual)
		assert.NotEqual(t, p1, p4)
		assert.Equal(t, 2, rb.Length())
	})
}
