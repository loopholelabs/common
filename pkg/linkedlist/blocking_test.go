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
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestBlockingLen(t *testing.T) {
	tests := []struct {
		name   string
		expect int
		before func(*Blocking[StringP, *StringP])
	}{
		{
			name:   "Works with no elements",
			expect: 0,
			before: func(dll *Blocking[StringP, *StringP]) {},
		},
		{
			name:   "Works with one element",
			expect: 1,
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("Test"))
				assert.NoError(t, err)
			},
		},
		{
			name:   "Works with 100 elements",
			expect: 100,
			before: func(dll *Blocking[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					_, err := dll.Push(NewStringP("Test"))
					assert.NoError(t, err)
				}
			},
		},
		{
			name:   "Works with one element backwards",
			expect: 1,
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("Test"))
				assert.NoError(t, err)
			},
		},
		{
			name:   "Works with 100 elements backwards",
			expect: 100,
			before: func(dll *Blocking[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					_, err := dll.Push(NewStringP("Test"))
					assert.NoError(t, err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewBlocking[StringP, *StringP]()

			tt.before(list)
			assert.Equal(t, tt.expect, list.Length())
		})
	}
}

func TestBlockingInsert(t *testing.T) {
	tests := []struct {
		name   string
		before func(*Blocking[StringP, *StringP])
		check  func(*Blocking[StringP, *StringP])
	}{
		{
			name:   "Works with no inserts",
			before: func(dll *Blocking[StringP, *StringP]) {},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 1 insert",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.EqualValues(t, dll.Drain(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Works with 2 inserts",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
				_, err = dll.Push(NewStringP("Two"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(2))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One"), NewStringP("Two")})
			},
		},
		{
			name: "Works with 100 inserts",
			before: func(dll *Blocking[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					_, err := dll.Push(NewStringP("Test"))
					assert.NoError(t, err)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(100))

				var expected []*StringP
				for i := 0; i < 100; i++ {
					expected = append(expected, NewStringP("Test"))
				}

				assert.Equal(t, dll.Drain(), expected)
			},
		},
		{
			name: "Works with 1 insert",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Works with 100 inserts",
			before: func(dll *Blocking[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					_, err := dll.Push(NewStringP(fmt.Sprintf("Test Backwards %d", i)))
					assert.NoError(t, err)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(100))

				var expected []*StringP
				for i := 0; i < 100; i++ {
					expected = append(expected, NewStringP(fmt.Sprintf("Test Backwards %d", i)))
				}

				assert.Equal(t, dll.Drain(), expected)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewBlocking[StringP, *StringP]()

			tt.before(list)

			tt.check(list)
		})
	}
}

func TestBlockingDelete(t *testing.T) {
	newNode := func(dll *Blocking[StringP, *StringP], val string) *list.Element {
		n, err := dll.Push(NewStringP(val))
		assert.NoError(t, err)
		return n
	}
	tests := []struct {
		name   string
		before func(*Blocking[StringP, *StringP]) []*list.Element
		apply  func(*Blocking[StringP, *StringP], []*list.Element)
		check  func(*Blocking[StringP, *StringP])
	}{
		{
			name: "Works with no inserts",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				return []*list.Element{}
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 1 insert",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"))
				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 2 inserts",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 100 inserts",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				var nodes []*list.Element

				for i := 0; i < 100; i++ {
					nodes = append(nodes, newNode(dll, "Test"))
				}

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Can delete the head",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				n1 := newNode(dll, "One")
				n2 := newNode(dll, "Two")

				nodes := append([]*list.Element{}, n1, n2)

				return nodes[0:1]
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))
				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("Two")})
			},
		},
		{
			name: "Can delete the tail",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes[1:]
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Can delete the same node multiple times",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return append([]*list.Element{nodes[1]}, nodes[1], nodes[1])
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, int(1), dll.Length())

				assert.Equal(t, []*StringP{NewStringP("One")}, dll.Drain())
			},
		},
		{
			name: "Works with 1 insert backwards",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"))
				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 2 inserts backwards",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))
				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 100 inserts backwards",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				var nodes []*list.Element

				for i := 0; i < 100; i++ {
					nodes = append(nodes, newNode(dll, "Test"))
				}

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Can delete the head backwards",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes[0:1]
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("Two")})
			},
		},
		{
			name: "Can delete the tail backwards",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes[1:]
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Can delete the same node multiple times",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return append([]*list.Element{nodes[1]}, nodes[1], nodes[1])
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, int(1), dll.Length())

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One")})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewBlocking[StringP, *StringP]()

			tt.apply(list, tt.before(list))

			tt.check(list)
		})
	}
}

func TestBlockingPop(t *testing.T) {
	tests := []struct {
		name    string
		before  func(*Blocking[StringP, *StringP])
		numPops int
		check   func(*Blocking[StringP, *StringP])
	}{
		{
			name:    "Hangs with no inserts until close",
			before:  func(dll *Blocking[StringP, *StringP]) {},
			numPops: 0,
			check: func(dll *Blocking[StringP, *StringP]) {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					_, err := dll.Pop()
					assert.ErrorIs(t, err, Closed)
					wg.Done()
				}()
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
				dll.Close()
				wg.Wait()
			},
		},
		{
			name:    "Hangs with no inserts until push",
			before:  func(dll *Blocking[StringP, *StringP]) {},
			numPops: 0,
			check: func(dll *Blocking[StringP, *StringP]) {
				n := NewStringP("One")
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					node, err := dll.Pop()
					assert.NoError(t, err)
					assert.Equal(t, n, node)
					wg.Done()
				}()
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
				_, err := dll.Push(n)
				assert.NoError(t, err)
				wg.Wait()
			},
		},
		{
			name: "Can insert and then double pop to cause a hang until push",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
			},
			numPops: 1,
			check: func(dll *Blocking[StringP, *StringP]) {
				n := NewStringP("Two")
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					node, err := dll.Pop()
					assert.NoError(t, err)
					assert.Equal(t, n, node)
					wg.Done()
				}()
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
				_, err := dll.Push(n)
				assert.NoError(t, err)
				wg.Wait()
			},
		},
		{
			name: "Can insert and then double pop to cause a hang until close",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
			},
			numPops: 1,
			check: func(dll *Blocking[StringP, *StringP]) {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					_, err := dll.Pop()
					assert.ErrorIs(t, err, Closed)
					wg.Done()
				}()
				assert.Equal(t, dll.Length(), int(0))
				assert.Equal(t, dll.Drain(), []*StringP{})
				dll.Close()
				wg.Wait()
			},
		},
		{
			name: "Works with 1 inserts and 1 shift",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
			},
			numPops: 1,
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
		{
			name: "Works with 3 inserts and 1 shift",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
				_, err = dll.Push(NewStringP("Two"))
				assert.NoError(t, err)
				_, err = dll.Push(NewStringP("Three"))
				assert.NoError(t, err)
			},
			numPops: 1,
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(2))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("Two"), NewStringP("Three")})
			},
		},
		{
			name: "Works with 3 inserts and 3 shifts",
			before: func(dll *Blocking[StringP, *StringP]) {
				_, err := dll.Push(NewStringP("One"))
				assert.NoError(t, err)
				_, err = dll.Push(NewStringP("Two"))
				assert.NoError(t, err)
				_, err = dll.Push(NewStringP("Three"))
				assert.NoError(t, err)
			},
			numPops: 3,
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(0))

				assert.Equal(t, dll.Drain(), []*StringP{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewBlocking[StringP, *StringP]()

			tt.before(list)

			for i := 0; i < tt.numPops; i++ {
				_, err := list.Pop()
				assert.NoError(t, err)
			}

			tt.check(list)
		})
	}
}

func TestBlockingBlockingIntegration(t *testing.T) {
	newNode := func(dll *Blocking[StringP, *StringP], val string) *list.Element {
		n, err := dll.Push(NewStringP(val))
		assert.NoError(t, err)
		return n
	}
	tests := []struct {
		name   string
		before func(*Blocking[StringP, *StringP]) []*list.Element
		apply  func(*Blocking[StringP, *StringP], []*list.Element)
		check  func(*Blocking[StringP, *StringP])
	}{
		{
			name: "Can delete something at the end and then insert again",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes[1:]
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}

				_, err := dll.Push(NewStringP("New"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(2))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("One"), NewStringP("New")})
			},
		},
		{
			name: "Can delete everything then insert again",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"))

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}

				_, err := dll.Push(NewStringP("New"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("New")})
			},
		},
		{
			name: "Can shift then insert",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"), newNode(dll, "Two"), newNode(dll, "Three"))

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				_, err := dll.Pop()
				assert.NoError(t, err)
				_, err = dll.Pop()
				assert.NoError(t, err)

				_, err = dll.Push(NewStringP("New"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(2))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("Three"), NewStringP("New")})
			},
		},
		{
			name: "Can insert one, then delete, then insert",
			before: func(dll *Blocking[StringP, *StringP]) []*list.Element {
				nodes := append([]*list.Element{}, newNode(dll, "One"))

				return nodes
			},
			apply: func(dll *Blocking[StringP, *StringP], n []*list.Element) {
				for _, node := range n {
					dll.Delete(node)
				}

				_, err := dll.Push(NewStringP("New"))
				assert.NoError(t, err)
			},
			check: func(dll *Blocking[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), int(1))

				assert.Equal(t, dll.Drain(), []*StringP{NewStringP("New")})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewBlocking[StringP, *StringP]()

			tt.apply(list, tt.before(list))

			tt.check(list)
		})
	}
}
