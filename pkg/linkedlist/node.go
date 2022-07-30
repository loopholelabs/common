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

type Pointer[T any] interface {
	*T
}

// Node is a container for data in the double linked list
type Node[T any, P Pointer[T]] struct {
	_padding0 [8]uint64 //nolint:structcheck,unused
	prev      *Node[T, P]
	_padding1 [8]uint64 //nolint:structcheck,unused
	next      *Node[T, P]
	_padding2 [8]uint64 //nolint:structcheck,unused
	value     P
}

// NewNode returns a pointer to a typed Node
func NewNode[T any, P Pointer[T]]() *Node[T, P] {
	return new(Node[T, P])
}

// Value returns the data stored in the node container
func (n *Node[T, P]) Value() P {
	return n.value
}

func (n *Node[T, P]) Reset() {
	n.prev = nil
	n.next = nil
	n.value = nil
}
