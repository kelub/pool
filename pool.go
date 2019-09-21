package pool

import (
	"context"
	"errors"
	"github.com/Sirupsen/logrus"
	"sync/atomic"
	"time"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

// NewPool create a new pool
// ctx , cancel
func NewPool(ctx context.Context, config *Config, factory Factory) (*ConnPool, error) {
	if config.MaxCap <= 0 {
		return nil, errors.New("invalid max capacity config")
	}
	if config.MaxCap < config.InitSize {
		config.InitSize = config.MaxCap
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &ConnPool{
		config:  config,
		factory: factory,
		cancel:  cancel,
		//fullCh:  make(chan struct{}),
	}
	if err := p.initPool(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *ConnPool) createItem(ctx context.Context) {
	logEntry := logrus.WithFields(logrus.Fields{
		"func_name": "createItem",
	})
	now := time.Now()
	conn, err := p.factory.New(ctx)
	if err != nil {
		logEntry.Errorln(err)
		// p.newItemCh <- nil
		return
	}
	item := &item{
		createdAt: now,
		conn:      conn,
	}
	atomic.AddInt64(&p.active, 1)
	p.newItemCh <- item
	return
}

func (p *ConnPool) newItemLoop(ctx context.Context) {
	logEntry := logrus.WithFields(logrus.Fields{
		"func_name": "newItemLoop",
	})
	for {
		select {
		case <-ctx.Done():
			logEntry.Debugln("Exit")
			return
		// May be blocked!
		// when closed newItemCh, newItemCh can not write.
		default:
			if p.active < p.config.MaxCap {
				p.createItem(ctx)
			}
		}
	}
}

// initPool init a Pool
// create InitSize number of  item to IdleItems.
func (p *ConnPool) initPool(ctx context.Context) error {
	now := time.Now()
	p.IdleItems = make(chan *item, p.config.MaxIdleCap)
	p.newItemCh = make(chan *item)
	for i := int64(0); i < p.config.InitSize; i++ {
		conn, err := p.factory.New(ctx)
		if err != nil {
			return err
		}
		item := &item{
			createdAt: now,
			conn:      conn,
		}
		//atomic.AddInt64(&p.active, 1)
		p.IdleItems <- item
		p.active++
	}

	go p.newItemLoop(ctx)
	return nil
}

// newItem  create new item
//func (p *ConnPool) newItem(ctx context.Context) (*item, error) {
//	logEntry := logrus.WithFields(logrus.Fields{
//		"func_name": "NewItem",
//	})
//	select {
//	case <-ctx.Done():
//		return nil, ctx.Err()
//	default:
//		now := time.Now()
//		conn, err := p.factory.New(ctx)
//		if err != nil {
//			return nil, err
//		}
//		item := &item{
//			createdAt: now,
//			conn:      conn,
//		}
//		logEntry.Infoln("item", item)
//		atomic.AddInt64(&p.active, 1)
//		return item, nil
//	}
//}

// Get  get a conn
func (p *ConnPool) Get(ctx context.Context, blockGet bool) (*Conn, error) {
	return p.getBlock(ctx)
}

/*
/*
getBlock
getBlock blocking gets an available object connection
Timeout is controlled by WaitTimeout
Priority is obtained from the idle channel IdleItems
IdleItems has no object Create new item
*/
func (p *ConnPool) getBlock(ctx context.Context) (*Conn, error) {
	logEntry := logrus.WithFields(logrus.Fields{
		"func_name": "GetBlock",
	})
	waitTimer := time.NewTimer(p.config.WaitTimeout)
	for {
		select {
		case item, ok := <-p.IdleItems:
			if !ok {
				logEntry.Infoln("IdleItems Closed")
				return nil, errors.New("Pool Closed")
			}
			if item == nil {
				logEntry.Infoln("IdleItems item nil")
				break
			}
			// check time
			if item.createdAt.Sub(time.Now()) > p.config.MaxIdleKeepTime {
				p.removeItem(item.conn)
				//Continue to loop until get a new item
				continue
			}
			//atomic.AddInt64(&p.active, 1)
			logEntry.Infoln("Get IdleItems", item.conn)
			return item.conn, nil
		case <-waitTimer.C:
			logEntry.Infoln("WaitTimeout")
			return nil, errors.New("WaitTimeout")
		case <-ctx.Done():
			logEntry.Infoln("ctx.Done()")
			return nil, ctx.Err()
		// make sure to use idle item first.
		default:
			if p.active < p.config.MaxCap {
				logEntry.Infoln("Get NewItem in")
				select{
				case <-waitTimer.C:
					logEntry.Infoln("WaitTimeout")
					return nil, errors.New("WaitTimeout")
				case <-ctx.Done():
					logEntry.Infoln("ctx.Done()")
					return nil, ctx.Err()
				case item, ok := <-p.newItemCh:
					if !ok {
						logEntry.Errorln("Pool Closed")
						return nil, errors.New("Pool Closed")
					}
					if item == nil {
						logEntry.Errorln("item is nil")
						return nil, errors.New("create Item error")
					}
					return item.conn, nil
				}
			}
		}
	}

}

// Put Recycle connection into the pool
// conn must be an object obtained in the pool
// blocking access
// PutWaitTimeout controls the timeout
// return err the item will be removed
func (p *ConnPool) Put(ctx context.Context, conn *Conn) error {
	logEntry := logrus.WithFields(logrus.Fields{
		"func_name": "Put",
	})
	now := time.Now()
	item := &item{
		createdAt: now,
		conn:      conn,
	}
	waitTimer := time.NewTimer(p.config.PutWaitTimeout)

	select {
	case p.IdleItems <- item:
		logEntry.Infoln("Put item: ", item)
		return nil
	case <-waitTimer.C:
		logEntry.Infoln("timeout")
		p.removeItem(item.conn)
		return errors.New("put item timeout")
	case <-ctx.Done():
		logEntry.Infoln("ctx Done, ", ctx.Err())
		p.removeItem(item.conn)
		return ctx.Err()
	}
}

// Destroy Destroy a conn
// conn must be an object obtained in the pool
func (p *ConnPool) Destroy(ctx context.Context, conn *Conn) error {
	p.removeItem(conn)
	return nil
}

// Close close pool
func (p *ConnPool) Close() {
	logEntry := logrus.WithFields(logrus.Fields{
		"func_name": "Close",
	})
	p.cancel()
	p.mu.Lock()
	if p.IdleItems != nil {
		p.mu.Unlock()
		close(p.IdleItems)
	}
	p.mu.Lock()
	if p.newItemCh != nil {
		p.mu.Unlock()
		// make sure p.newItemCh is empty
		select{
		case <-p.newItemCh:
			close(p.newItemCh)
		default:
			close(p.newItemCh)
		}
	}
	logEntry.Infoln("Closed Pool")
}

func (p *ConnPool) removeItem(conn *Conn) {
	atomic.AddInt64(&p.active, -1)
	p.factory.Close(conn)
}
