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
	"container/list"
	"sync"
)

// Blocking is a Blocking double-linked list that will
// block when the list is empty until either a node is added
// or the list is closed.
type Blocking[T any, P Pointer[T]] struct {
	_padding0 [8]uint64 //nolint:structcheck,unused
	lock      *sync.Mutex
	_padding1 [8]uint64 //nolint:structcheck,unused
	notEmpty  *sync.Cond
	_padding2 [8]uint64 //nolint:structcheck,unused
	closed    bool
	_padding3 [8]uint64 //nolint:structcheck,unused
	list      *list.List
}

// NewBlocking creates a new Blocking double-linked list that can function as a
// FIFO queue when used with the Push and Pop methods.
func NewBlocking[T any, P Pointer[T]]() *Blocking[T, P] {
	l := new(Blocking[T, P])
	l.lock = new(sync.Mutex)
	l.notEmpty = sync.NewCond(l.lock)
	l.list = list.New()
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
func (l *Blocking[T, P]) Length() (len int) {
	l.lock.Lock()
	len = l.list.Len()
	l.lock.Unlock()
	return
}

// Push adds a new node at the end of the Blocking linked list
func (l *Blocking[T, P]) Push(val P) (*list.Element, error) {
	l.lock.Lock()
	if l.isClosed() {
		l.lock.Unlock()
		return nil, Closed
	}
	element := l.list.PushBack(val)
	l.notEmpty.Signal()
	l.lock.Unlock()
	return element, nil
}

// Delete removes a node from the Blocking linked list
func (l *Blocking[T, P]) Delete(e *list.Element) {
	l.lock.Lock()
	l.list.Remove(e)
	l.lock.Unlock()
}

// Pop removes and returns the node from the start of the Blocking linked list
func (l *Blocking[T, P]) Pop() (P, error) {
	l.lock.Lock()
LOOP:
	if l.isClosed() {
		l.lock.Unlock()
		return nil, Closed
	}
	if l.list.Len() == 0 {
		l.notEmpty.Wait()
		goto LOOP
	}
	e := l.list.Front()
	l.list.Remove(e)
	l.lock.Unlock()
	return e.Value.(P), nil
}

// Drain removes all elements from the list.
// and returns them in a slice.
//
// This function should only be called after the list is closed.
func (l *Blocking[T, P]) Drain() (out []P) {
	l.lock.Lock()
	out = []P{}
	el := l.list.Front()
	for el != nil {
		out = append(out, el.Value.(P))
		el = el.Next()
	}
	l.lock.Unlock()
	return
}
