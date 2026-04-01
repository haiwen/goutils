package clusterkit

import "iter"

type Iter[V any] interface {
	Range() iter.Seq[V]
	// Error reports any error that caused the returned sequence to stop
	// iterating. Always call Error() after iteration finishes.
	Error() error
}

type iterator[V any] struct {
	seq func(func(V) bool) error
	err error
}

func NewIter[V any](seq func(yield func(V) bool) error) Iter[V] {
	return &iterator[V]{seq: seq}
}

func (it *iterator[V]) Range() iter.Seq[V] {
	return func(yield func(V) bool) {
		it.err = it.seq(yield)
	}
}

func (it *iterator[V]) Error() error {
	return it.err
}
