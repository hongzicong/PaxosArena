package client

import (
	"errors"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type ReqReply struct {
	Val    state.Value
	Seqnum int
	Time   time.Time
}

type BufferClient struct {
	*Client

	Reply chan *ReqReply

	psize       int
	writes      int
	arrivalRate float64
	keyCount    int
	zipfSkew    float64
	warmup      time.Duration
	duration    time.Duration

	rand *rand.Rand
}

type zipfParameters struct {
	keyCount int
	skew     float64
}

var zipfCDFCache = struct {
	sync.Mutex
	values map[zipfParameters][]float64
}{
	values: make(map[zipfParameters][]float64),
}

func NewBufferClient(c *Client, psize, writes, keyCount int, zipfSkew float64) *BufferClient {
	bc := &BufferClient{
		Client: c,

		Reply: make(chan *ReqReply, 1024),

		psize:    psize,
		writes:   writes,
		keyCount: keyCount,
		zipfSkew: zipfSkew,
	}
	source := rand.NewSource(time.Now().UnixNano() + int64(c.ClientId))
	bc.rand = rand.New(source)
	return bc
}

func (c *BufferClient) PoissonArrivals(arrivalRate float64) {
	c.arrivalRate = arrivalRate
}

func (c *BufferClient) MeasureFor(warmup, duration time.Duration) {
	c.warmup = warmup
	c.duration = duration
}

func (c *BufferClient) RegisterReply(val state.Value, seqnum int32) {
	t := time.Now()
	c.Reply <- &ReqReply{
		Val:    val,
		Seqnum: int(seqnum),
		Time:   t,
	}
}

func (c *BufferClient) Write(key int64, val []byte) {
	c.SendWrite(key, val)
	<-c.Reply
	return
}

func (c *BufferClient) Read(key int64) []byte {
	c.SendRead(key)
	r := <-c.Reply
	return r.Val
}

func (c *BufferClient) Scan(key, count int64) []byte {
	c.SendScan(key, count)
	r := <-c.Reply
	return r.Val
}

// Assumed to be connected
func (c *BufferClient) Loop() {
	getKey := c.genGetKey()
	val := make([]byte, c.psize)
	c.rand.Read(val)
	c.loopOpen(getKey, val)
}

type scheduledRequest struct {
	key      int64
	write    bool
	measured bool
}

type requestTiming struct {
	sentAt   time.Time
	measured bool
}

func (c *BufferClient) loopOpen(getKey func() int64, val []byte) {
	c.Printf("Open-loop Poisson arrival rate %v requests/second\n", c.arrivalRate)
	c.Printf("Warm-up %v, measurement duration %v\n", c.warmup, c.duration)

	c.sendScheduledRequest(scheduledRequest{
		key:   getKey(),
		write: c.randomTrue(c.writes),
	}, val)
	<-c.Reply

	requests := make(chan scheduledRequest, 1024)
	stopReplies := make(chan struct{})
	var senders sync.WaitGroup
	var replies sync.WaitGroup
	var timings sync.Map
	measuredRequests := 0
	warmupRequests := 0

	go func() {
		for {
			select {
			case reply := <-c.Reply:
				value, exists := timings.LoadAndDelete(reply.Seqnum)
				if !exists {
					continue
				}
				timing := value.(requestTiming)
				if timing.measured {
					d := reply.Time.Sub(timing.sentAt)
					milliseconds := float64(d.Nanoseconds()) / float64(time.Millisecond)
					c.Println("Returning:", reply.Val.String())
					c.Printf("latency %v\n", milliseconds)
				}
				replies.Done()
			case <-stopReplies:
				return
			}
		}
	}()

	senders.Add(1)
	go func() {
		defer senders.Done()
		for request := range requests {
			sentAt := time.Now()
			sequence := int(c.seqnum + 1)
			timings.Store(sequence, requestTiming{
				sentAt:   sentAt,
				measured: request.measured,
			})
			replies.Add(1)
			c.sendScheduledRequest(request, val)
		}
	}()

	start := time.Now()
	warmupEnd := start.Add(c.warmup)
	measurementEnd := warmupEnd.Add(c.duration)
	nextArrival := start
	for {
		nextArrival = nextArrival.Add(c.poissonInterval())
		if !nextArrival.Before(measurementEnd) {
			break
		}
		if delay := time.Until(nextArrival); delay > 0 {
			time.Sleep(delay)
		}

		measured := !nextArrival.Before(warmupEnd)
		if measured {
			measuredRequests++
		} else {
			warmupRequests++
		}
		requests <- scheduledRequest{
			key:      getKey(),
			write:    c.randomTrue(c.writes),
			measured: measured,
		}
	}
	close(requests)
	senders.Wait()
	replies.Wait()
	close(stopReplies)

	c.Printf("Warm-up requests %d\n", warmupRequests)
	c.Printf("Measured requests %d\n", measuredRequests)
	c.Printf("Test took %v\n", time.Since(start))
	c.Disconnect()
}

func (c *BufferClient) poissonInterval() time.Duration {
	seconds := c.rand.ExpFloat64() / c.arrivalRate
	return time.Duration(seconds * float64(time.Second))
}

func (c *BufferClient) sendScheduledRequest(request scheduledRequest, val []byte) {
	if request.write {
		c.SendWrite(request.key, state.Value(val))
	} else {
		c.SendRead(request.key)
	}
}

func (c *BufferClient) WaitReplies(waitFrom int) {
	go func() {
		for {
			r, err := c.GetReplyFrom(waitFrom)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					c.Println("warning: calling GetReplyFrom after closing connections. Not a big deal")
				} else {
					c.Println(err)
				}
				break
			}
			if r.OK != defs.TRUE {
				c.Println("Faulty reply")
				break
			}
			go func(val state.Value, seqnum int32) {
				time.Sleep(c.dt.WaitDuration(c.replicas[waitFrom]))
				c.RegisterReply(val, seqnum)
			}(r.Value, r.CommandId)
		}
	}()
}

func (c *BufferClient) genGetKey() func() int64 {
	cdf := sharedZipfCDF(c.keyCount, c.zipfSkew)
	c.Printf("Zipfian key distribution: keyCount=%d skew=%v\n", c.keyCount, c.zipfSkew)
	getKey := func() int64 {
		return int64(sort.SearchFloat64s(cdf, c.rand.Float64()))
	}
	return getKey
}

func sharedZipfCDF(keyCount int, skew float64) []float64 {
	parameters := zipfParameters{
		keyCount: keyCount,
		skew:     skew,
	}

	zipfCDFCache.Lock()
	defer zipfCDFCache.Unlock()
	if cdf, exists := zipfCDFCache.values[parameters]; exists {
		return cdf
	}

	cdf := make([]float64, keyCount)
	total := 0.0
	for key := 1; key <= keyCount; key++ {
		total += math.Pow(float64(key), -skew)
		cdf[key-1] = total
	}
	for key := range cdf {
		cdf[key] /= total
	}
	cdf[len(cdf)-1] = 1
	zipfCDFCache.values[parameters] = cdf
	return cdf
}

func (c *BufferClient) randomTrue(prob int) bool {
	if prob >= 100 {
		return true
	}
	if prob > 0 {
		return c.rand.Intn(100) <= prob
	}
	return false
}
