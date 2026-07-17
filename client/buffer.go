package client

import (
	"errors"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/imdea-software/swiftpaxos/replica/defs"
	"github.com/imdea-software/swiftpaxos/state"
)

type ReqReply struct {
	Val    state.Value
	Seqnum int
	Time   time.Time
}

type BufferClient struct {
	*Client

	Reply chan *ReqReply

	seq         bool
	psize       int
	reqNum      int
	writes      int
	window      int32
	syncFreq    int
	arrivalRate float64
	keyCount    int
	zipfSkew    float64
	warmup      time.Duration
	duration    time.Duration

	reqTime    []time.Time
	launchTime time.Time

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

func NewBufferClient(c *Client, reqNum, psize, writes, keyCount int, zipfSkew float64) *BufferClient {
	bc := &BufferClient{
		Client: c,

		Reply: make(chan *ReqReply, reqNum+1),

		seq:      true,
		psize:    psize,
		reqNum:   reqNum,
		writes:   writes,
		keyCount: keyCount,
		zipfSkew: zipfSkew,

		reqTime: make([]time.Time, reqNum+1),
	}
	source := rand.NewSource(time.Now().UnixNano() + int64(c.ClientId))
	bc.rand = rand.New(source)
	return bc
}

func (c *BufferClient) Pipeline(syncFreq int, window int32) {
	c.seq = false
	c.syncFreq = syncFreq
	c.window = window
}

func (c *BufferClient) PoissonArrivals(arrivalRate float64) {
	c.seq = false
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
	if c.arrivalRate > 0 {
		if c.duration > 0 {
			c.loopOpenTimed(getKey, val)
			return
		}
		c.loopOpen(getKey, val)
		return
	}

	var cmdM sync.Mutex
	cmdNum := int32(0)
	wait := make(chan struct{}, 0)
	go func() {
		for i := 0; i <= c.reqNum; i++ {
			r := <-c.Reply
			// Ignore first request
			if i != 0 {
				d := r.Time.Sub(c.reqTime[r.Seqnum])
				m := float64(d.Nanoseconds()) / float64(time.Millisecond)
				c.Println("Returning:", r.Val.String())
				c.Printf("latency %v\n", m)
			}
			if c.window > 0 {
				cmdM.Lock()
				if cmdNum == c.window {
					cmdNum--
					cmdM.Unlock()
					wait <- struct{}{}
				} else {
					cmdNum--
					cmdM.Unlock()
				}
			}
			if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
				wait <- struct{}{}
			}
		}
		if !c.seq {
			wait <- struct{}{}
		}
	}()

	for i := 0; i <= c.reqNum; i++ {
		key := getKey()
		write := c.randomTrue(c.writes)
		c.reqTime[i] = time.Now()

		// Ignore first request
		if i == 1 {
			c.launchTime = c.reqTime[i]
		}

		if write {
			c.SendWrite(key, state.Value(val))
			// TODO: if the return value != i, something's wrong
		} else {
			c.SendRead(key)
			// TODO: if the return value != i, something's wrong
		}
		if c.window > 0 {
			cmdM.Lock()
			if cmdNum == c.window-1 {
				cmdNum++
				cmdM.Unlock()
				<-wait
			} else {
				cmdNum++
				cmdM.Unlock()
			}
		}
		if c.seq || (c.syncFreq > 0 && i%c.syncFreq == 0) {
			<-wait
		}
	}

	if !c.seq {
		<-wait
	}

	c.Printf("Test took %v\n", time.Now().Sub(c.launchTime))
	c.Disconnect()
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

	c.reqTime[0] = time.Now()
	c.sendScheduledRequest(scheduledRequest{
		key:   getKey(),
		write: c.randomTrue(c.writes),
	}, val)
	<-c.Reply

	if c.reqNum == 0 {
		c.Printf("Test took %v\n", time.Duration(0))
		c.Disconnect()
		return
	}

	requests := make(chan scheduledRequest, c.reqNum)
	go func() {
		for request := range requests {
			c.sendScheduledRequest(request, val)
		}
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < c.reqNum; i++ {
			r := <-c.Reply
			d := r.Time.Sub(c.reqTime[r.Seqnum])
			m := float64(d.Nanoseconds()) / float64(time.Millisecond)
			c.Println("Returning:", r.Val.String())
			c.Printf("latency %v\n", m)
		}
		close(done)
	}()

	nextArrival := time.Now()
	for i := 1; i <= c.reqNum; i++ {
		nextArrival = nextArrival.Add(c.poissonInterval())
		if delay := time.Until(nextArrival); delay > 0 {
			time.Sleep(delay)
		}

		c.reqTime[i] = time.Now()
		if i == 1 {
			c.launchTime = c.reqTime[i]
		}
		requests <- scheduledRequest{
			key:   getKey(),
			write: c.randomTrue(c.writes),
		}
	}
	close(requests)

	<-done
	c.Printf("Test took %v\n", time.Since(c.launchTime))
	c.Disconnect()
}

func (c *BufferClient) loopOpenTimed(getKey func() int64, val []byte) {
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
