// Package admit is admission control: a global concurrency limit with a
// bounded FIFO wait queue (requests wait at most MaxWait, then are shed)
// and per-tenant concurrency limits.
package admit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueFull   = errors.New("queue full")
	ErrWaitTimeout = errors.New("queued too long")
	ErrTenantLimit = errors.New("tenant concurrency limit")
)

type Controller struct {
	mu        sync.Mutex
	limit     int // global in-flight limit (changes with the replica count)
	inflight  int
	queue     []*waiter
	MaxQueue  int
	MaxWait   time.Duration
	perTenant map[string]int
	tenantCap map[string]int
	Shed      int64
}

type waiter struct {
	ch     chan struct{}
	tenant string
}

func New(limit, maxQueue int, maxWait time.Duration, tenantCaps map[string]int) *Controller {
	return &Controller{limit: limit, MaxQueue: maxQueue, MaxWait: maxWait, perTenant: map[string]int{}, tenantCap: tenantCaps}
}

func (c *Controller) SetLimit(n int) {
	c.mu.Lock()
	c.limit = n
	c.grant()
	c.mu.Unlock()
}

func (c *Controller) Stats() (inflight, queued, limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight, len(c.queue), c.limit
}

// grant admits queued waiters while there is room (caller holds mu).
func (c *Controller) grant() {
	for len(c.queue) > 0 && c.inflight < c.limit {
		w := c.queue[0]
		c.queue = c.queue[1:]
		c.inflight++
		c.perTenant[w.tenant]++
		close(w.ch)
	}
}

// Acquire admits a request or returns an error after at most MaxWait.
func (c *Controller) Acquire(ctx context.Context, tenant string) error {
	c.mu.Lock()
	if cap, ok := c.tenantCap[tenant]; ok && c.perTenant[tenant] >= cap {
		c.Shed++
		c.mu.Unlock()
		return ErrTenantLimit
	}
	if c.inflight < c.limit && len(c.queue) == 0 {
		c.inflight++
		c.perTenant[tenant]++
		c.mu.Unlock()
		return nil
	}
	if len(c.queue) >= c.MaxQueue {
		c.Shed++
		c.mu.Unlock()
		return ErrQueueFull
	}
	w := &waiter{ch: make(chan struct{}), tenant: tenant}
	c.queue = append(c.queue, w)
	c.mu.Unlock()
	t := time.NewTimer(c.MaxWait)
	defer t.Stop()
	select {
	case <-w.ch:
		return nil
	case <-t.C:
	case <-ctx.Done():
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-w.ch: // granted while timing out: keep it
		return nil
	default:
	}
	for i, x := range c.queue {
		if x == w {
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			break
		}
	}
	c.Shed++
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrWaitTimeout
}

func (c *Controller) Release(tenant string) {
	c.mu.Lock()
	c.inflight--
	c.perTenant[tenant]--
	c.grant()
	c.mu.Unlock()
}
