// Originated from https://github.com/ross-oreto/go-tree/blob/master/btree.go with the following changes:
// 1. Genericized the code
// 2. Added Match function for our frame sorter use case
// 3. Fixed a bug in deleteNode where in some cases the deleted flag was not set to true

/*
Copyright (c) 2017 Ross Oreto

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package tree

import (
	"fmt"
)

type Val[T any] interface {
	Comp(val T) int8   // returns 1 if > val, -1 if < val, 0 if equals to val
	Match(cond T) int8 // returns 1 if > cond, -1 if < cond, 0 if matches cond
}

const maxNodePoolSize = 512

// Btree represents an AVL tree
type Btree[T Val[T]] struct {
	root     *Node[T]
	values   []T
	len      int
	nodePool []*Node[T]
}

// Node represents a node in the tree with a value, left and right children, and a height/balance of the node.
type Node[T Val[T]] struct {
	Value       T
	left, right *Node[T]
	height      int8
}

// New returns a new btree
func New[T Val[T]]() *Btree[T] { return new(Btree[T]).Init() }

// Init initializes all values/clears the tree and returns the tree pointer
func (t *Btree[T]) Init() *Btree[T] {
	t.root = nil
	t.values = nil
	t.len = 0
	t.nodePool = t.nodePool[:0]
	return t
}

func (t *Btree[T]) allocNode(value T) *Node[T] {
	if len(t.nodePool) > 0 {
		n := t.nodePool[len(t.nodePool)-1]
		t.nodePool = t.nodePool[:len(t.nodePool)-1]
		n.Value = value
		n.left = nil
		n.right = nil
		n.height = 1
		return n
	}
	return (&Node[T]{Value: value, height: 1})
}

func (t *Btree[T]) freeNode(n *Node[T]) {
	var zero T
	n.Value = zero
	n.left = nil
	n.right = nil
	n.height = 0
	if len(t.nodePool) < maxNodePoolSize {
		t.nodePool = append(t.nodePool, n)
	}
}

// String returns a string representation of the tree values
func (t *Btree[T]) String() string {
	return fmt.Sprint(t.Values())
}

// Empty returns true if the tree is empty
func (t *Btree[T]) Empty() bool {
	return t.root == nil
}

// NotEmpty returns true if the tree is not empty
func (t *Btree[T]) NotEmpty() bool {
	return t.root != nil
}

// Insert inserts a new value into the tree and returns the tree pointer
func (t *Btree[T]) Insert(value T) *Btree[T] {
	added := false
	t.root = t.insert(t.root, value, &added)
	if added {
		t.len++
	}
	t.values = nil
	return t
}

func (t *Btree[T]) insert(n *Node[T], value T, added *bool) *Node[T] {
	if n == nil {
		*added = true
		return t.allocNode(value)
	}
	c := value.Comp(n.Value)
	if c > 0 {
		n.right = t.insert(n.right, value, added)
	} else if c < 0 {
		n.left = t.insert(n.left, value, added)
	} else {
		n.Value = value
		*added = false
		return n
	}

	n.height = n.maxHeight() + 1
	c = balance(n)

	if c > 1 {
		c = value.Comp(n.left.Value)
		if c < 0 {
			return n.rotateRight()
		} else if c > 0 {
			n.left = n.left.rotateLeft()
			return n.rotateRight()
		}
	} else if c < -1 {
		c = value.Comp(n.right.Value)
		if c > 0 {
			return n.rotateLeft()
		} else if c < 0 {
			n.right = n.right.rotateRight()
			return n.rotateLeft()
		}
	}
	return n
}

// InsertAll inserts all the values into the tree and returns the tree pointer
func (t *Btree[T]) InsertAll(values []T) *Btree[T] {
	for _, v := range values {
		t.Insert(v)
	}
	return t
}

// Contains returns true if the tree contains the specified value
func (t *Btree[T]) Contains(value T) bool {
	return t.Get(value) != nil
}

// ContainsAny returns true if the tree contains any of the values
func (t *Btree[T]) ContainsAny(values []T) bool {
	for _, v := range values {
		if t.Contains(v) {
			return true
		}
	}
	return false
}

// ContainsAll returns true if the tree contains all of the values
func (t *Btree[T]) ContainsAll(values []T) bool {
	for _, v := range values {
		if !t.Contains(v) {
			return false
		}
	}
	return true
}

// Get returns the node value associated with the search value
func (t *Btree[T]) Get(value T) *T {
	var node *Node[T]
	if t.root != nil {
		node = t.root.get(value)
	}
	if node != nil {
		return &node.Value
	}
	return nil
}

func (t *Btree[T]) Match(cond T) []T {
	var matches []T
	if t.root != nil {
		t.root.match(cond, &matches)
	}
	return matches
}

// Len return the number of nodes in the tree
func (t *Btree[T]) Len() int {
	return t.len
}

// Head returns the first value in the tree
func (t *Btree[T]) Head() *T {
	if t.root == nil {
		return nil
	}
	beginning := t.root
	for beginning.left != nil {
		beginning = beginning.left
	}
	if beginning == nil {
		for beginning.right != nil {
			beginning = beginning.right
		}
	}
	if beginning != nil {
		return &beginning.Value
	}
	return nil
}

// Tail returns the last value in the tree
func (t *Btree[T]) Tail() *T {
	if t.root == nil {
		return nil
	}
	beginning := t.root
	for beginning.right != nil {
		beginning = beginning.right
	}
	if beginning == nil {
		for beginning.left != nil {
			beginning = beginning.left
		}
	}
	if beginning != nil {
		return &beginning.Value
	}
	return nil
}

// Values returns a slice of all the values in tree in order
func (t *Btree[T]) Values() []T {
	if t.values == nil {
		t.values = make([]T, t.len)
		t.Ascend(func(n *Node[T], i int) bool {
			t.values[i] = n.Value
			return true
		})
	}
	return t.values
}

// Delete deletes the node from the tree associated with the search value
func (t *Btree[T]) Delete(value T) *Btree[T] {
	deleted := false
	t.root = t.deleteNode(t.root, value, &deleted)
	if deleted {
		t.len--
	}
	t.values = nil
	return t
}

// DeleteAll deletes the nodes from the tree associated with the search values
func (t *Btree[T]) DeleteAll(values []T) *Btree[T] {
	for _, v := range values {
		t.Delete(v)
	}
	return t
}

func (t *Btree[T]) deleteNode(n *Node[T], value T, deleted *bool) *Node[T] {
	if n == nil {
		return n
	}

	c := value.Comp(n.Value)

	if c < 0 {
		n.left = t.deleteNode(n.left, value, deleted)
	} else if c > 0 {
		n.right = t.deleteNode(n.right, value, deleted)
	} else {
		if n.left == nil {
			r := n.right
			t.freeNode(n)
			*deleted = true
			return r
		} else if n.right == nil {
			l := n.left
			t.freeNode(n)
			*deleted = true
			return l
		}
		m := n.right.min()
		n.Value = m.Value
		n.right = t.deleteNode(n.right, m.Value, deleted)
		*deleted = true
	}

	// re-balance
	if n == nil {
		return n
	}
	n.height = n.maxHeight() + 1
	bal := balance(n)
	if bal > 1 {
		if balance(n.left) >= 0 {
			return n.rotateRight()
		}
		n.left = n.left.rotateLeft()
		return n.rotateRight()
	} else if bal < -1 {
		if balance(n.right) <= 0 {
			return n.rotateLeft()
		}
		n.right = n.right.rotateRight()
		return n.rotateLeft()
	}

	return n
}

// Pop deletes the last node from the tree and returns its value
func (t *Btree[T]) Pop() *T {
	value := t.Tail()
	if value != nil {
		v := *value
		t.Delete(v)
		return &v
	}
	return nil
}

// Pull deletes the first node from the tree and returns its value
func (t *Btree[T]) Pull() *T {
	value := t.Head()
	if value != nil {
		v := *value
		t.Delete(v)
		return &v
	}
	return nil
}

// NodeIterator expresses the iterator function used for traversals
type NodeIterator[T Val[T]] func(n *Node[T], i int) bool

// Ascend performs an ascending order traversal of the tree calling the iterator function on each node
// the iterator will continue as long as the NodeIterator returns true
func (t *Btree[T]) Ascend(iterator NodeIterator[T]) {
	var i int
	if t.root != nil {
		t.root.iterate(iterator, &i, true)
	}
}

// Descend performs a descending order traversal of the tree using the iterator
// the iterator will continue as long as the NodeIterator returns true
func (t *Btree[T]) Descend(iterator NodeIterator[T]) {
	var i int
	if t.root != nil {
		t.root.rIterate(iterator, &i, true)
	}
}

// Debug prints out useful debug information about the tree for debugging purposes
func (t *Btree[T]) Debug() {
	fmt.Println("----------------------------------------------------------------------------------------------")
	if t.Empty() {
		fmt.Println("tree is empty")
	} else {
		fmt.Println(t.Len(), "elements")
	}

	t.Ascend(func(n *Node[T], i int) bool {
		if t.root.Value.Comp(n.Value) == 0 {
			fmt.Print("ROOT ** ")
		}
		n.Debug()
		return true
	})
	fmt.Println("----------------------------------------------------------------------------------------------")
}

// Init initializes the values of the node or clears the node and returns the node pointer
func (n *Node[T]) Init() *Node[T] {
	n.height = 1
	n.left = nil
	n.right = nil
	return n
}

// String returns a string representing the node
func (n *Node[T]) String() string {
	return fmt.Sprint(n.Value)
}

// Debug prints out useful debug information about the tree node for debugging purposes
func (n *Node[T]) Debug() {
	var children string
	if n.left == nil && n.right == nil {
		children = "no children |"
	} else if n.left != nil && n.right != nil {
		children = fmt.Sprint("left child:", n.left.String(), " right child:", n.right.String())
	} else if n.right != nil {
		children = fmt.Sprint("right child:", n.right.String())
	} else {
		children = fmt.Sprint("left child:", n.left.String())
	}

	fmt.Println(n.String(), "|", "height", n.height, "|", "balance", balance(n), "|", children)
}

func height[T Val[T]](n *Node[T]) int8 {
	if n != nil {
		return n.height
	}
	return 0
}

func balance[T Val[T]](n *Node[T]) int8 {
	if n == nil {
		return 0
	}
	return height(n.left) - height(n.right)
}

func (n *Node[T]) get(val T) *Node[T] {
	var node *Node[T]
	c := val.Comp(n.Value)
	if c < 0 {
		if n.left != nil {
			node = n.left.get(val)
		}
	} else if c > 0 {
		if n.right != nil {
			node = n.right.get(val)
		}
	} else {
		node = n
	}
	return node
}

func (n *Node[T]) match(cond T, results *[]T) {
	c := n.Value.Match(cond)
	if c > 0 {
		if n.left != nil {
			n.left.match(cond, results)
		}
	} else if c < 0 {
		if n.right != nil {
			n.right.match(cond, results)
		}
	} else {
		// other matching nodes could be on both sides
		if n.left != nil {
			n.left.match(cond, results)
		}
		*results = append(*results, n.Value)
		if n.right != nil {
			n.right.match(cond, results)
		}
	}
}

func (n *Node[T]) rotateRight() *Node[T] {
	l := n.left
	// Rotation
	l.right, n.left = n, l.right

	// update heights
	n.height = n.maxHeight() + 1
	l.height = l.maxHeight() + 1

	return l
}

func (n *Node[T]) rotateLeft() *Node[T] {
	r := n.right
	// Rotation
	r.left, n.right = n, r.left

	// update heights
	n.height = n.maxHeight() + 1
	r.height = r.maxHeight() + 1

	return r
}

func (n *Node[T]) iterate(iterator NodeIterator[T], i *int, cont bool) {
	if n != nil && cont {
		n.left.iterate(iterator, i, cont)
		cont = iterator(n, *i)
		*i++
		n.right.iterate(iterator, i, cont)
	}
}

func (n *Node[T]) rIterate(iterator NodeIterator[T], i *int, cont bool) {
	if n != nil && cont {
		n.right.iterate(iterator, i, cont)
		cont = iterator(n, *i)
		*i++
		n.left.iterate(iterator, i, cont)
	}
}

func (n *Node[T]) min() *Node[T] {
	current := n
	for current.left != nil {
		current = current.left
	}
	return current
}

func (n *Node[T]) maxHeight() int8 {
	rh := height(n.right)
	lh := height(n.left)
	if rh > lh {
		return rh
	}
	return lh
}
