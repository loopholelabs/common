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
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type StringP string

func NewStringP(s string) *StringP {
	return (*StringP)(&s)
}

func TestLen(t *testing.T) {
	tests := []struct {
		name   string
		expect uint64
		before func(*Double[StringP, *StringP])
	}{
		{
			name:   "Works with no elements",
			expect: 0,
			before: func(dll *Double[StringP, *StringP]) {},
		},
		{
			name:   "Works with one element",
			expect: 1,
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("Test"))
			},
		},
		{
			name:   "Works with 100 elements",
			expect: 100,
			before: func(dll *Double[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					dll.Push(NewStringP("Test"))
				}
			},
		},
		{
			name:   "Works with one element backwards",
			expect: 1,
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("Test"))
			},
		},
		{
			name:   "Works with 100 elements backwards",
			expect: 100,
			before: func(dll *Double[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					dll.Push(NewStringP("Test"))
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewDouble[StringP, *StringP]()

			tt.before(list)
			assert.Equal(t, tt.expect, list.Length())
		})
	}
}

func TestInsert(t *testing.T) {
	tests := []struct {
		name   string
		before func(*Double[StringP, *StringP])
		check  func(*Double[StringP, *StringP])
	}{
		{
			name:   "Works with no inserts",
			before: func(dll *Double[StringP, *StringP]) {},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))
				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 1 insert",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.EqualValues(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Works with 2 inserts",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
				dll.Push(NewStringP("Two"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(2))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One"), NewStringP("Two")})
			},
		},
		{
			name: "Works with 100 inserts",
			before: func(dll *Double[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					dll.Push(NewStringP("Test"))
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(100))

				var expected []*StringP
				for i := 0; i < 100; i++ {
					expected = append(expected, NewStringP("Test"))
				}

				assert.Equal(t, dll.toArray(), expected)
			},
		},
		{
			name: "Works with 1 insert backwards",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Works with 2 inserts backwards",
			before: func(dll *Double[StringP, *StringP]) {
				dll.PushBack(NewStringP("One"))
				dll.PushBack(NewStringP("Two"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(2))
				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("Two"), NewStringP("One")})
			},
		},
		{
			name: "Works with 100 inserts backwards",
			before: func(dll *Double[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					dll.PushBack(NewStringP("Test"))
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(100))

				var expected []*StringP
				for i := 0; i < 100; i++ {
					expected = append(expected, NewStringP("Test"))
				}

				assert.Equal(t, dll.toArray(), expected)
			},
		},
		{
			name: "Works with 100 inserts backwards and forwards",
			before: func(dll *Double[StringP, *StringP]) {
				for i := 0; i < 100; i++ {
					dll.PushBack(NewStringP(fmt.Sprintf("Test Backwards %d", i)))
					dll.Push(NewStringP(fmt.Sprintf("Test Forward %d", i)))
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(200))

				var expected []*StringP
				for i := 99; i > -1; i-- {
					expected = append(expected, NewStringP(fmt.Sprintf("Test Backwards %d", i)))
				}
				for i := 0; i < 100; i++ {
					expected = append(expected, NewStringP(fmt.Sprintf("Test Forward %d", i)))
				}

				assert.Equal(t, dll.toArray(), expected)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewDouble[StringP, *StringP]()

			tt.before(list)

			tt.check(list)
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name   string
		before func(*Double[StringP, *StringP]) []*Node[StringP, *StringP]
		apply  func(*Double[StringP, *StringP], []*Node[StringP, *StringP])
		check  func(*Double[StringP, *StringP])
	}{
		{
			name: "Works with no inserts",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				return []*Node[StringP, *StringP]{}
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))
				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 1 insert",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")))
				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 2 inserts",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 100 inserts",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				var nodes []*Node[StringP, *StringP]

				for i := 0; i < 100; i++ {
					nodes = append(nodes, dll.Push(NewStringP("Test")))
				}

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Can delete the head",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes[0:1]
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("Two")})
			},
		},
		{
			name: "Can delete the tail",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes[1:]
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Can delete the same node multiple times",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return append([]*Node[StringP, *StringP]{nodes[1]}, nodes[1], nodes[1])
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Works with 1 insert backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")))
				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 2 inserts backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))
				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))
				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 100 inserts backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				var nodes []*Node[StringP, *StringP]

				for i := 0; i < 100; i++ {
					nodes = append(nodes, dll.Push(NewStringP("Test")))
				}

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Can delete the head backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes[0:1]
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("Two")})
			},
		},
		{
			name: "Can delete the tail backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes[1:]
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
		{
			name: "Can delete the same node multiple times backwards",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return append([]*Node[StringP, *StringP]{nodes[1]}, nodes[1], nodes[1])
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One")})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewDouble[StringP, *StringP]()

			tt.apply(list, tt.before(list))

			tt.check(list)
		})
	}
}

func TestPopFirst(t *testing.T) {
	tests := []struct {
		name    string
		before  func(*Double[StringP, *StringP])
		shiftBy int
		check   func(*Double[StringP, *StringP])
	}{
		{
			name:    "Works with no inserts",
			before:  func(dll *Double[StringP, *StringP]) {},
			shiftBy: 0,
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name:    "Works with no inserts and 5 shifts",
			before:  func(dll *Double[StringP, *StringP]) {},
			shiftBy: 5,
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 1 inserts and 1 shift",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
			},
			shiftBy: 1,
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
		{
			name: "Works with 3 inserts and 1 shift",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
				dll.Push(NewStringP("Two"))
				dll.Push(NewStringP("Three"))
			},
			shiftBy: 1,
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(2))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("Two"), NewStringP("Three")})
			},
		},
		{
			name: "Works with 3 inserts and 3 shifts",
			before: func(dll *Double[StringP, *StringP]) {
				dll.Push(NewStringP("One"))
				dll.Push(NewStringP("Two"))
				dll.Push(NewStringP("Three"))
			},
			shiftBy: 3,
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(0))

				assert.Equal(t, dll.toArray(), []*StringP{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewDouble[StringP, *StringP]()

			tt.before(list)

			for i := 0; i < tt.shiftBy; i++ {
				list.Pop()
			}

			tt.check(list)
		})
	}
}

func TestDoubleIntegration(t *testing.T) {
	tests := []struct {
		name   string
		before func(*Double[StringP, *StringP]) []*Node[StringP, *StringP]
		apply  func(*Double[StringP, *StringP], []*Node[StringP, *StringP])
		check  func(*Double[StringP, *StringP])
	}{
		{
			name: "Can delete something at the end and then insert again",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes[1:]
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}

				dll.Push(NewStringP("New"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(2))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("One"), NewStringP("New")})
			},
		},
		{
			name: "Can delete everything then insert again",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")))

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}

				dll.Push(NewStringP("New"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("New")})
			},
		},
		{
			name: "Can shift then insert",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")), dll.Push(NewStringP("Two")), dll.Push(NewStringP("Three")))

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				dll.Pop()
				dll.Pop()

				dll.Push(NewStringP("New"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(2))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("Three"), NewStringP("New")})
			},
		},
		{
			name: "Can insert one, then delete, then insert",
			before: func(dll *Double[StringP, *StringP]) []*Node[StringP, *StringP] {
				nodes := append([]*Node[StringP, *StringP]{}, dll.Push(NewStringP("One")))

				return nodes
			},
			apply: func(dll *Double[StringP, *StringP], n []*Node[StringP, *StringP]) {
				for _, node := range n {
					dll.Delete(node)
				}

				dll.Push(NewStringP("New"))
			},
			check: func(dll *Double[StringP, *StringP]) {
				assert.Equal(t, dll.Length(), uint64(1))

				assert.Equal(t, dll.toArray(), []*StringP{NewStringP("New")})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := NewDouble[StringP, *StringP]()

			tt.apply(list, tt.before(list))

			tt.check(list)
		})
	}
}
