/*
	Copyright 2022 Loophole Labs

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		   http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

package linkedlist

import (
	"github.com/loopholelabs/common/pkg/pool"
	"sync"
)

// Blocking is a Blocking double-linked list that will
// block when the list is empty until either a node is added
// or the list is closed.
type Blocking[T any, P Pointer[T]] struct {
	_padding0 [8]uint64 //nolint:structcheck,unused
	lock      *sync.Mutex
	_padding1 [8]uint64 //nolint:structcheck,unused
	head      *Node[T, P]
	_padding2 [8]uint64 //nolint:structcheck,unused
	tail      *Node[T, P]
	_padding3 [8]uint64 //nolint:structcheck,unused
	len       uint64
	_padding4 [8]uint64 //nolint:structcheck,unused
	closed    bool
	_padding5 [8]uint64 //nolint:structcheck,unused
	notEmpty  *sync.Cond
	_padding6 [8]uint64 //nolint:structcheck,unused
	pool      *pool.Pool[Node[T, P], *Node[T, P]]
}

// NewBlocking creates a new Blocking double-linked list that can function as a
// FIFO queue when used with the Push and Pop methods.
func NewBlocking[T any, P Pointer[T]]() *Blocking[T, P] {
	l := new(Blocking[T, P])
	l.lock = new(sync.Mutex)
	l.notEmpty = sync.NewCond(l.lock)
	l.pool = pool.NewPool[Node[T, P], *Node[T, P]](NewNode[T, P])
	return l
}

// IsClosed returns true if the list is closed. After the list
// is closed, it will no longer accept new nodes.
//
// The Drain method can be used to drain the list after it is closed.
func (l *Blocking[T, P]) IsClosed() (closed bool) {
	l.lock.Lock()
	closed = l.isClosed()
	l.lock.Unlock()
	return
}

// isClosed is an internal method that returns true if the list is closed.
func (l *Blocking[T, P]) isClosed() bool {
	return l.closed
}

// Close closes the list. After the list is closed, it will no longer accept new
// nodes or allow nodes to be popped. Nodes can still be deleted.
//
// The Drain method can be used to drain the list after it is closed.
func (l *Blocking[T, P]) Close() {
	l.lock.Lock()
	l.closed = true
	l.notEmpty.Broadcast()
	l.lock.Unlock()
}

// Length returns the count of nodes stored in the Blocking linked list
func (l *Blocking[T, P]) Length() (len uint64) {
	l.lock.Lock()
	len = l.len
	l.lock.Unlock()
	return
}

// Push adds a new node at the end of the Blocking linked list
func (l *Blocking[T, P]) Push(val P) (*Node[T, P], error) {
	node := l.pool.Get()
	node.value = val
	l.lock.Lock()
	if l.isClosed() {
		l.lock.Unlock()
		l.pool.Put(node)
		return nil, Closed
	}

	if l.head == nil {
		l.head = node
	}

	if l.tail == nil {
		l.tail = node
	} else {
		l.tail.next = node
		node.prev = l.tail
		l.tail = node
	}

	l.len++
	l.notEmpty.Signal()
	l.lock.Unlock()
	return node, nil
}

// Delete removes a node from the Blocking linked list
func (l *Blocking[T, P]) Delete(node *Node[T, P]) {
	decrement := false
	l.lock.Lock()
	if node == l.head {
		l.head = node.next
		decrement = true
	}
	if node == l.tail {
		l.tail = node.prev
		decrement = true
	}
	if node.next != nil {
		node.next.prev = node.prev
		decrement = true
	}
	if node.prev != nil {
		node.prev.next = node.next
		decrement = true
	}
	node.next = nil
	node.prev = nil
	if decrement {
		l.len--
	}
	l.lock.Unlock()
	l.pool.Put(node)
}

// Pop removes and returns the node from the start of the Blocking linked list
func (l *Blocking[T, P]) Pop() (P, error) {
	l.lock.Lock()
LOOP:
	if l.isClosed() {
		l.lock.Unlock()
		return nil, Closed
	}
	if l.len == 0 || l.head == nil {
		l.notEmpty.Wait()
		goto LOOP
	}

	node := l.head
	l.head = l.head.next
	if l.head != nil {
		l.head.prev = node.prev
	}
	l.len--
	val := node.Value()
	l.pool.Put(node)
	l.lock.Unlock()

	return val, nil
}

// Drain removes all elements from the list.
// and returns them in a slice.
//
// This function should only be called after the list is closed.
func (l *Blocking[T, P]) Drain() (out []P) {
	l.lock.Lock()
	out = []P{}
	el := l.head
	for el != nil {
		out = append(out, el.Value())
		el = el.next
	}
	l.lock.Unlock()
	return
}
