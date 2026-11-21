package receiver

// ring is a bounded FIFO buffer: pushing into a full ring drops the oldest item.
type ring[T any] struct {
	buf   []T
	start int
	n     int
}

func newRing[T any](size int) ring[T] { return ring[T]{buf: make([]T, size)} }

// push adds v and returns the dropped item, if any.
func (r *ring[T]) push(v T) (T, bool) {
	var old T
	if r.n < len(r.buf) {
		r.buf[(r.start+r.n)%len(r.buf)] = v
		r.n++
		return old, false
	}
	old = r.buf[r.start]
	r.buf[r.start] = v
	r.start = (r.start + 1) % len(r.buf)
	return old, true
}

// items returns a copy of the content, oldest first.
func (r *ring[T]) items() []T {
	out := make([]T, 0, r.n)
	for i := 0; i < r.n; i++ {
		out = append(out, r.buf[(r.start+i)%len(r.buf)])
	}
	return out
}
