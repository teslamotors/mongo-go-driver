// Copyright (C) MongoDB, Inc. 2017-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package topology

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/address"
)

const maxConnections = uint(6000)

// ErrPoolConnected is returned from an attempt to connect an already connected pool
var ErrPoolConnected = PoolError("attempted to Connect to an already connected pool")

// ErrPoolDisconnected is returned from an attempt to Close an already disconnected
// or disconnecting pool.
var ErrPoolDisconnected = PoolError("attempted to check out a connection from closed connection pool")

// ErrPoolMinimumIODeadlineNotMet is returned if a connection would be used with little to no context deadline left
var ErrPoolMinimumIODeadlineNotMet = PoolError("remaining context deadline did not meet the minimum needed for IO")

// ErrConnectionClosed is returned from an attempt to use an already closed connection.
var ErrConnectionClosed = ConnectionError{ConnectionID: "<closed>", message: "connection is closed"}

// ErrWrongPool is return when a connection is returned to a pool it doesn't belong to.
var ErrWrongPool = PoolError("connection does not belong to this pool")

// PoolError is an error returned from a Pool method.
type PoolError string

// maintainInterval is the interval at which the manager background routine will open and close connections.
var maintainInterval = 10 * time.Millisecond

// defaultExpireConnectionRatio is the ratio of opened/inuse which needs to be exceeded to begin closing connections
var defaultExpireConnectionRatio = float32(3.0)

// defaultExpireConnectionFrequency is the duration we should wait between demand related connection expiration
var defaultExpireConnectionFrequency = 30 * time.Second

// statsInterval is the frequency at which stats will be published
var statsInterval = 10 * time.Second

func (pe PoolError) Error() string { return string(pe) }

// poolConfig contains all aspects of the pool that can be configured
type poolConfig struct {
	Address                   address.Address
	ExpireConnectionFrequency *time.Duration
	ExpireConnectionRatio     *float32
	MinPoolSize               uint64
	MaxPoolSize               uint64 // MaxPoolSize is not used because handling the max number of connections in the pool is handled in server. This is only used for command monitoring
	MaxIdleTime               time.Duration
	PoolMonitor               *event.PoolMonitor
}

// checkOutResult is all the values that can be returned from a checkOut
type checkOutResult struct {
	c      *connection
	err    error
	reason string
}

// pool is a wrapper of resource pool that follows the CMAP spec for connection pools
type pool struct {
	address                   address.Address
	opts                      []ConnectionOption
	generation                *poolGenerationMap
	expireConnectionFrequency time.Duration
	expireConnectionRatio     float32
	monitor                   *event.PoolMonitor
	maxSize                   uint
	minSize                   uint

	poolChan    chan *connection
	closingChan chan struct{}
	inUse       uint32
	waiting     uint32

	// Must be accessed using the atomic package.
	connected                    int32
	pinnedCursorConnections      uint64
	pinnedTransactionConnections uint64

	nextid uint64
	opened map[uint64]*connection // opened holds all of the currently open connections.
	sync.RWMutex
}

// expiredConnection checks if a given connection is stale and should be removed from the resource pool
func (p *pool) expiredConnection(c *connection) bool {
	if c == nil {
		return true
	}

	switch {
	case atomic.LoadInt32(&c.pool.connected) != connected:
		c.expireReason = event.ReasonPoolClosed
	case c.closed():
		// A connection would only be closed if it encountered a network error during an operation and closed itself.
		c.expireReason = event.ReasonConnectionErrored
	case c.idleTimeoutExpired():
		c.expireReason = event.ReasonIdle
	case p.stale(c):
		c.expireReason = event.ReasonStale
	default:
		return false
	}

	return true
}

// newPool creates a new pool that will hold size number of idle connections. It will use the
// provided options when creating connections.
func newPool(config poolConfig, connOpts ...ConnectionOption) (*pool, error) {
	opts := connOpts
	if config.MaxIdleTime != time.Duration(0) {
		opts = append(opts, WithIdleTimeout(func(_ time.Duration) time.Duration { return config.MaxIdleTime }))
	}
	if config.PoolMonitor != nil {
		opts = append(opts, withPoolMonitor(func(_ *event.PoolMonitor) *event.PoolMonitor { return config.PoolMonitor }))
	}

	var maxConns = uint(config.MaxPoolSize)
	if maxConns == 0 || maxConns > maxConnections {
		maxConns = maxConnections
	}

	expireConnectionFrequency := defaultExpireConnectionFrequency
	if config.ExpireConnectionFrequency != nil {
		expireConnectionFrequency = *config.ExpireConnectionFrequency
	}

	expireConnectionRatio := defaultExpireConnectionRatio
	if config.ExpireConnectionRatio != nil {
		expireConnectionRatio = *config.ExpireConnectionRatio
	}
	pool := &pool{
		address:                   config.Address,
		expireConnectionFrequency: expireConnectionFrequency,
		expireConnectionRatio:     expireConnectionRatio,
		maxSize:                   maxConns,
		minSize:                   uint(config.MinPoolSize),
		monitor:                   config.PoolMonitor,
		connected:                 disconnected,
		opened:                    make(map[uint64]*connection),
		opts:                      opts,
		poolChan:                  make(chan *connection, maxConns),
		generation:                newPoolGenerationMap(),
	}
	pool.opts = append(pool.opts, withGenerationNumberFn(func(_ generationNumberFn) generationNumberFn { return pool.getGenerationForNewConnection }))

	if pool.monitor != nil {
		pool.monitor.Event(&event.PoolEvent{
			Type: event.PoolCreated,
			PoolOptions: &event.MonitorPoolOptions{
				MaxPoolSize:        uint64(pool.maxSize),
				MinPoolSize:        uint64(pool.minSize),
				WaitQueueTimeoutMS: uint64(config.MaxIdleTime) / uint64(time.Millisecond),
			},
			Address: pool.address.String(),
		})
	}

	return pool, nil
}

// stale checks if a given connection's generation is below the generation of the pool
func (p *pool) stale(c *connection) bool {
	return c == nil || p.generation.stale(c.desc.ServiceID, c.generation)
}

// connect puts the pool into the connected state, allowing it to be used and will allow items to begin being processed from the wait queue
func (p *pool) connect() error {
	if !atomic.CompareAndSwapInt32(&p.connected, disconnected, connected) {
		return ErrPoolConnected
	}
	p.generation.connect()

	for i := uint(0); i < p.minSize; i++ {
		if err := p.addConnection(); err != nil {
			return err
		}
	}

	p.closingChan = make(chan struct{})
	if p.monitor != nil {
		go p.statsPublisher(p.closingChan)
	}
	go p.manager(p.closingChan)
	return nil
}

// addConnection will create and add a new connection
func (p *pool) addConnection() error {
	c, err := newConnection(p.address, p.opts...)
	if err != nil {
		return err
	}

	c.pool = p
	c.poolID = atomic.AddUint64(&p.nextid, 1)

	if p.monitor != nil {
		p.monitor.Event(&event.PoolEvent{
			Type:         event.ConnectionCreated,
			Address:      p.address.String(),
			ConnectionID: c.poolID,
		})
	}

	if atomic.LoadInt32(&p.connected) != connected {
		// Manually publish a ConnectionClosed event here because the connection reference hasn't been stored and we
		// need to ensure each ConnectionCreated event has a corresponding ConnectionClosed event.
		if p.monitor != nil {
			p.monitor.Event(&event.PoolEvent{
				Type:         event.ConnectionClosed,
				Address:      p.address.String(),
				ConnectionID: c.poolID,
				Reason:       event.ReasonPoolClosed,
			})
		}
		p.closeConnection(c) // The pool is disconnected or disconnecting, ignore the error from closing the connection.
		return ErrPoolDisconnected
	}

	p.Lock()
	p.opened[c.poolID] = c
	p.Unlock()

	go p.connectAndAdd(c)

	return nil
}

// connectAndAdd connects and then adds the connection to the buffered channel
func (p *pool) connectAndAdd(c *connection) {
	start := time.Now()
	c.connect(context.Background())
	err := c.wait()
	if err != nil {
		if p.monitor != nil {
			p.monitor.Event(&event.PoolEvent{
				Type:         event.ConnectionClosed,
				Address:      p.address.String(),
				ConnectionID: c.poolID,
				Reason:       event.ReasonTimedOut,
			})
		}

		p.Lock()
		delete(p.opened, c.poolID)
		p.Unlock()
		return
	}

	if p.monitor != nil {
		elapsed := time.Since(start)
		p.monitor.Event(&event.PoolEvent{
			Type:         event.ConnectionEstablished,
			Address:      p.address.String(),
			ConnectionID: c.poolID,
			Duration:     &elapsed,
			PoolStats:    p.poolStats(),
		})
	}

	p.poolChan <- c
}

func (p *pool) statsPublisher(doneChan chan struct{}) {
	ticker := time.NewTicker(statsInterval)
	for {
		select {
		case <-doneChan:
			return
		case <-ticker.C:
			p.monitor.Event(&event.PoolEvent{
				Type:      event.PoolStatsEvent,
				Address:   p.address.String(),
				PoolStats: p.poolStats(),
			})
		}
	}
}

// manager spins up connections or reduces connections as needed
func (p *pool) manager(doneChan chan struct{}) {
	ticker := time.NewTicker(maintainInterval)
	lastCloseTime := time.Now()
	for {
		select {
		case <-doneChan:
			return
		case <-ticker.C:
			if p.connectionNeeded() {
				p.addConnection()
			} else if time.Since(lastCloseTime) > p.expireConnectionFrequency && p.connectionGlut() {
				go p.decrementConnections()
				lastCloseTime = time.Now()
			}
		}
	}
}

// decrementConnections synchronously removes a connection from the pool
func (p *pool) decrementConnections() {
	c := <-p.poolChan
	c.expireReason = event.ReasonPoolReduction
	p.removeConnection(c)
	_ = p.closeConnection(c)
}

func (p *pool) connectionNeeded() bool {
	p.RLock()
	defer p.RUnlock()
	return atomic.LoadUint32(&p.waiting) > 0 && uint(len(p.opened)) < p.maxSize
}

func (p *pool) numOpened() uint {
	p.RLock()
	defer p.RUnlock()
	return uint(len(p.opened))
}

func (p *pool) connectionGlut() bool {
	p.RLock()
	defer p.RUnlock()
	opened := uint(len(p.opened))
	return float32(opened) > p.expireConnectionRatio*(float32(p.inUse+1)) && opened > p.minSize
}

func (p *pool) poolStats() *event.PoolStats {
	p.RLock()
	defer p.RUnlock()
	return &event.PoolStats{
		InUse:   p.inUse,
		Opened:  uint32(len(p.opened)),
		Waiting: p.waiting,
	}
}

// get returns a connection from the pool
func (p *pool) get(ctx context.Context) (*connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()

	if atomic.LoadInt32(&p.connected) != connected {
		if p.monitor != nil {
			p.monitor.Event(&event.PoolEvent{
				Type:    event.GetFailed,
				Address: p.address.String(),
				Reason:  event.ReasonPoolClosed,
			})
		}
		return nil, ErrPoolDisconnected
	}

	// Attempt to get a connection, increment waiting if we don't get one
	for {
		select {
		case c := <-p.poolChan:
			if !p.processGetConnection(c, start) {
				continue
			}
			return p.checkMinimumIODuration(ctx, c)
		default:
			atomic.AddUint32(&p.waiting, 1)
			defer func() {
				// decrement
				atomic.AddUint32(&p.waiting, ^uint32(0))
			}()
		}
		break
	}

	// Block until we get a connection
	for {
		select {
		case <-ctx.Done():
			if p.monitor != nil {
				elapsed := time.Since(start)
				p.monitor.Event(&event.PoolEvent{
					Type:     event.GetFailed,
					Address:  p.address.String(),
					Reason:   event.ReasonTimedOut,
					Duration: &elapsed,
				})
			}
			return nil, ctx.Err()
		case c := <-p.poolChan:
			if !p.processGetConnection(c, start) {
				continue
			}
			return p.checkMinimumIODuration(ctx, c)
		}
	}
}

func (p *pool) checkMinimumIODuration(ctx context.Context, c *connection) (*connection, error) {
	if c.minIODuration > 0 {
		if deadline, ok := ctx.Deadline(); ok {
			if time.Until(deadline) < c.minIODuration {
				if p.monitor != nil {
					p.monitor.Event(&event.PoolEvent{
						Type:    event.GetFailed,
						Address: p.address.String(),
						Reason:  event.ReasonMinimumIO,
					})
				}
				p.put(c)
				return nil, ErrPoolMinimumIODeadlineNotMet
			}
		}
	}
	return c, nil
}

// processGetConnection expires connections and writes monitoring events for a connection pulled from the buffered channel
// returns false if the connection was bad
func (p *pool) processGetConnection(c *connection, start time.Time) bool {
	if p.expiredConnection(c) {
		p.removeConnection(c)
		go p.closeConnection(c)
		return false
	}

	if p.monitor != nil {
		elapsed := time.Since(start)
		p.monitor.Event(&event.PoolEvent{
			Type:         event.GetSucceeded,
			Address:      p.address.String(),
			ConnectionID: c.poolID,
			Duration:     &elapsed,
		})
	}

	atomic.AddUint32(&p.inUse, 1)
	c.checkoutTime = time.Now()
	return true
}

// closeConnection closes a connection, not the pool itself. This method will actually closeConnection the connection,
// making it unusable, to instead return the connection to the pool, use put.
func (p *pool) closeConnection(c *connection) error {
	if c.pool != p {
		return ErrWrongPool
	}

	if atomic.LoadInt32(&c.connected) == connected {
		c.closeConnectContext()
		_ = c.wait() // Make sure that the connection has finished connecting
	}

	if !atomic.CompareAndSwapInt32(&c.connected, connected, disconnected) {
		return nil // We're closing an already closed connection
	}

	if c.nc != nil {
		err := c.nc.Close()
		if err != nil {
			return ConnectionError{ConnectionID: c.id, Wrapped: err, message: "failed to close net.Conn"}
		}
	}

	return nil
}

func (p *pool) getGenerationForNewConnection(serviceID *primitive.ObjectID) uint64 {
	return p.generation.addConnection(serviceID)
}

// removeConnection removes a connection from the pool.
func (p *pool) removeConnection(c *connection) {
	var publishEvent bool
	p.Lock()
	if _, ok := p.opened[c.poolID]; ok {
		publishEvent = true
		delete(p.opened, c.poolID)
	}
	p.Unlock()

	// Only update the generation numbers map if the connection has retrieved its generation number. Otherwise, we'd
	// decrement the count for the generation even though it had never been incremented.
	if c.hasGenerationNumber() {
		p.generation.removeConnection(c.desc.ServiceID)
	}

	if publishEvent && p.monitor != nil {
		c.pool.monitor.Event(&event.PoolEvent{
			Type:         event.ConnectionClosed,
			Address:      c.pool.address.String(),
			ConnectionID: c.poolID,
			Reason:       c.expireReason,
			PoolStats:    p.poolStats(),
		})
	}
}

// put returns a connection to this pool. If the pool is connected, the connection is not
// stale, and there is space in the cache, the connection is returned to the cache.
func (p *pool) put(c *connection) error {
	if c == nil {
		if p.monitor != nil {
			p.monitor.Event(&event.PoolEvent{
				Type: event.ConnectionReturned,
			})
		}
		return nil
	}

	if p.monitor != nil {
		elapsed := time.Since(c.checkoutTime)
		p.monitor.Event(&event.PoolEvent{
			Type:         event.ConnectionReturned,
			ConnectionID: c.poolID,
			Address:      c.addr.String(),
			Duration:     &elapsed,
		})
	}

	if c.pool != p {
		return ErrWrongPool
	}

	atomic.AddUint32(&p.inUse, ^uint32(0))
	p.poolChan <- c

	return nil
}

// gracefulDrain drains the pool from the buffer channel, allowing connections to finish their work
func (p *pool) gracefulDrain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
		case c := <-p.poolChan:
			c.expireReason = event.ReasonPoolClosed
			p.removeConnection(c)
			go p.closeConnection(c)
			if p.numOpened() > 0 {
				continue
			}
		}
		break
	}
}

// closeConnections hard closes connections, use gracefulDrain first if there is a deadline
func (p *pool) closeConnections() error {
	// We copy the remaining connections into a slice, then iterate it to close them. This allows us
	// to use a single function to actually clean up and close connections at the expense of a
	// double iteration in the worse case.
	p.Lock()
	toClose := make([]*connection, 0, len(p.opened))
	for _, c := range p.opened {
		toClose = append(toClose, c)
	}
	p.Unlock()
	var err error
	for _, c := range toClose {
		c.expireReason = event.ReasonPoolClosed
		p.removeConnection(c)
		err = p.closeConnection(c) // We call close synchronously here, and will return the last error
	}

	return err
}

// disconnect the pool and closes all connections
func (p *pool) disconnect(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&p.connected, connected, disconnecting) {
		return ErrPoolDisconnected
	}
	close(p.closingChan)

	if ctx == nil {
		ctx = context.Background()
	}
	p.generation.disconnect()

	if _, ok := ctx.Deadline(); ok {
		p.gracefulDrain(ctx)
	}

	// Close any connections that were not closed during graceful shutdown
	err := p.closeConnections()

	atomic.StoreInt32(&p.connected, disconnected)

	if p.monitor != nil {
		p.monitor.Event(&event.PoolEvent{
			Type:    event.PoolClosedEvent,
			Address: p.address.String(),
		})
	}
	return err
}

func (p *pool) pinConnectionToCursor() {
	atomic.AddUint64(&p.pinnedCursorConnections, 1)
}

func (p *pool) unpinConnectionFromCursor() {
	// See https://golang.org/pkg/sync/atomic/#AddUint64 for an explanation of the ^uint64(0) syntax.
	atomic.AddUint64(&p.pinnedCursorConnections, ^uint64(0))
}

func (p *pool) pinConnectionToTransaction() {
	atomic.AddUint64(&p.pinnedTransactionConnections, 1)
}

func (p *pool) unpinConnectionFromTransaction() {
	// See https://golang.org/pkg/sync/atomic/#AddUint64 for an explanation of the ^uint64(0) syntax.
	atomic.AddUint64(&p.pinnedTransactionConnections, ^uint64(0))
}

// clear clears the pool by incrementing the generation
func (p *pool) clear(serviceID *primitive.ObjectID) {
	if p.monitor != nil {
		p.monitor.Event(&event.PoolEvent{
			Type:      event.PoolCleared,
			Address:   p.address.String(),
			ServiceID: serviceID,
		})
	}
	p.generation.clear(serviceID)
}
