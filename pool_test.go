package pool

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

type ConnFactory struct {
	id int64
}

type Conn struct {
	id int64
	//c net.Conn
}

func (c *ConnFactory) New(ctx context.Context) (io.Closer, error) {
	//id := c.id + 1
	return &Conn{}, nil
}

func (c *ConnFactory) Close(conn io.Closer) error {
	return conn.Close()
}

func (c *Conn) Handle() {
	//fmt.Println("Conn Handle id: ", c.id)
	//rand.Seed(time.Now().UnixNano())
	//nums := 500 - rand.Intn(200)
	nums := 400
	time.Sleep(time.Duration(nums) * time.Millisecond)
}

func (c *Conn) Close()error{
	fmt.Println("Close")
	return nil
}

func GetConfig() *Config {
	return &Config{
		MaxCap:          int64(10),
		MaxIdleCap:      int64(10),
		InitSize:        int64(10),
		WaitTimeout:     1 * time.Second,
		PutWaitTimeout:  10 * time.Millisecond,
		BlockGet:        true,
		MaxIdleKeepTime: 30 * time.Minute,
	}
}

func Test_PoolGetTimeOut(t *testing.T) {
	poolCtx := context.Background()
	config := GetConfig()
	p, err := NewPool(poolCtx, config, &ConnFactory{})
	assert.Nil(t, err)
	assert.Equal(t, int64(len(p.IdleItems)), config.InitSize)
	ctx, cancel := context.WithCancel(context.Background())

	_, err = p.Get(ctx, true)
	assert.Nil(t, err)

	for i := 0; i < int(p.config.MaxIdleCap)-1; i++ {
		<-p.IdleItems
	}

	//close(p.IdleItems)
	p.active = p.config.MaxCap

	// go func() {
	// 	time.Sleep(4 * time.Second)
	// 	// p.IdleItems <- &item{conn: &Conn{id: int64(99)}}
	// 	cancel()
	// }()

	_, err = p.Get(ctx, true)

	//assert.Nil(t, err)
	//assert.Equal(t, i.id, int64(99))
	assert.NotNil(t, err)

	cancel()
	//pool.Close()
}

func Test_PoolGetNewTtem(t *testing.T) {
	poolCtx := context.Background()
	config := GetConfig()
	config.InitSize = config.InitSize - 5
	p, err := NewPool(poolCtx, config, &ConnFactory{})
	assert.Nil(t, err)
	assert.Equal(t, int64(len(p.IdleItems)), config.InitSize)
	ctx, cancel := context.WithCancel(context.Background())

	_, err = p.Get(ctx, true)
	assert.Nil(t, err)
	//assert.Equal(t, i.id, int64(1))

	for i := 0; i < int(p.config.InitSize)-1; i++ {
		<-p.IdleItems
	}

	//close(p.IdleItems)
	//p.active = p.config.MaxCap

	//go func() {
	//	time.Sleep(4 * time.Second)
	//	//p.IdleItems <- &item{conn: &Conn{id: int64(99)}}
	//	cancel()
	//}()

	_, err = p.Get(ctx, true)

	assert.Nil(t, err)
	//fmt.Println("err:", err)

	cancel()
	p.Close()
}

func Test_PoolPut(t *testing.T) {
	config := GetConfig()
	poolCtx := context.Background()

	p, err := NewPool(poolCtx, config, &ConnFactory{})
	assert.Nil(t, err)
	assert.Equal(t, int64(len(p.IdleItems)), config.InitSize)
	ctx, cancel := context.WithCancel(context.Background())

	i, err := p.Get(ctx, true)
	assert.Nil(t, err)

	err = p.Put(ctx, i)
	assert.Nil(t, err)

	i2, err := p.Get(ctx, true)
	assert.Nil(t, err)
	err = p.Destroy(ctx, i2)
	assert.Nil(t, err)
	cancel()
	//pool.Close()
}

func GetConfig2() *Config {
	return &Config{
		MaxCap:          int64(2000),
		MaxIdleCap:      int64(2000),
		InitSize:        int64(20),
		WaitTimeout:     20 * time.Millisecond,
		PutWaitTimeout:  20 * time.Millisecond,
		BlockGet:        true,
		MaxIdleKeepTime: 3 * time.Minute,
	}
}

func Benchmark_PoolParallel(b *testing.B) {
	config := GetConfig2()
	poolCtx := context.Background()

	p, err := NewPool(poolCtx, config, &ConnFactory{})
	assert.Nil(b, err)
	assert.Equal(b, int64(len(p.IdleItems)), config.InitSize)
	// ctx, cancel := context.WithCancel(context.Background())
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := p.Get(ctx, true)
			if err != nil {
				b.Error(err)
				continue
			}
			conn.(*Conn).Handle()
			err = p.Put(ctx, conn)
			if err != nil {
				b.Error(err)
				continue
			}
		}
	})
}
