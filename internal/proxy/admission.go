package proxy

// Admission caps requests in flight upstream. One Admission is meant to be
// shared by every Handler in the gateway, so the cap, and with it the memory
// bound of cap × max body size (ADR 0001), holds across all routes and
// upstreams rather than per route.
type Admission struct {
	slots chan struct{} // counting semaphore
}

// NewAdmission returns an Admission allowing n requests at once. n <= 0
// selects DefaultMaxConcurrent.
func NewAdmission(n int) *Admission {
	if n <= 0 {
		n = DefaultMaxConcurrent
	}
	return &Admission{slots: make(chan struct{}, n)}
}

// TryAcquire takes a slot if one is free and reports whether it did. It never
// waits: at the cap the caller sheds the request rather than queueing it.
func (a *Admission) TryAcquire() bool {
	select {
	case a.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release frees a slot taken by a successful TryAcquire.
func (a *Admission) Release() { <-a.slots }
