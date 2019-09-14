package pool

import (
	"context"
	"sync"
	"time"
)

const (
	DefaultMaxCap  = 10
	DefaultInitCap = 10

	//
	DefaultWaitTimeout    = 100 * time.Millisecond
	DefaultPutWaitTimeout = 100 * time.Millisecond

	DefaultMaxIdleKeepTime = 30 * time.Minute
	DefaultClearIdleTime   = 10 * time.Minute
)

type Pool interface {
	Get(ctx context.Context, blockGet bool) (*Conn, error)
	Put(ctx context.Context, conn *Conn) error
	Destroy(ctx context.Context, conn *Conn) error
	Close()
}

type Factory interface {
	New(ctx context.Context) (*Conn, error)
	Close(*Conn) error
}

type Conn struct {
	id int64
}

type Config struct {
	// pool Maximum capacity
	MaxCap int64
	// Maximum Idle capacity
	MaxIdleCap int64

	// init size
	InitSize int64
	// get item timeout (BlockGet set true)
	WaitTimeout time.Duration
	// put itemto pool timeout
	PutWaitTimeout time.Duration

	// Whether to block the get object
	BlockGet bool

	// Maximum idle object retention time. More than will be removed.
	MaxIdleKeepTime time.Duration
	// Clear idle object interval
	ClearIdleTime time.Duration
}

type ConnPool struct {
	config  *Config
	factory Factory
	mu      sync.RWMutex
	ctx     context.Context
	cancel  func()
	// Number of objects created
	active    int64
	IdleItems chan *item
	//fullCh    chan struct{}
}

type item struct {
	createdAt time.Time
	conn      *Conn
}
