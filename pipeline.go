package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// NoMoreData is the "error" a source DoWork() method should return when it doesn't have any more data
type NoMoreData struct {
}

func (e NoMoreData) Error() string {
	return "Not an error: no more data"
}

// InvalidInputType is the error a pipeline element DoWork() method should return when it receive data with the wrong type
type InvalidInputType struct {
	Expected	string
	Actual		string
}

func (e InvalidInputType) Error() string {
	return fmt.Sprintf("Invalid input format: expected = %s, actual = %s", e.Expected, e.Actual)
}

// Source represents an element on the pipeline which has no input
type Source interface {
	DoWork(ctx context.Context) (interface{}, error)
}

// Filter represents an element on the pipeline which has both input and output
type Filter interface {
	MaxWorkers() int
	DoWork(ctx context.Context, input interface{}) (interface{}, error)
}

// Sink represents an element on  the pipeline which has no output
type Sink interface {
	DoWork(ctx context.Context, input interface{}) error
}

// Stats stores statistics about an element in a running pipeline
type Stats struct {
	Time		int64
	Count		int64
	Mutex		sync.RWMutex
}

func (s Stats) String() string {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
	var avg int64
	if s.Count > 0 {
		avg = int64(s.Time / s.Count)
	}
	return fmt.Sprintf("Total DoWork calls: %d\tAverage Time: %d ns", s.Count, avg)
}

// Pipeline represents a pipeline object
type Pipeline struct {
	source			Source
	sourceStats		Stats
	filters			[]Filter
	filtersStats	[]Stats
	sink			Sink
	sinkStats		Stats
}

// MakePipeline creates and initialize a new pipeline object
func MakePipeline() *Pipeline {
	return &Pipeline{filters: make([]Filter, 0)}
}

// SetSource sets the pipeline source
func (p *Pipeline) SetSource(source Source) *Pipeline {
	p.source = source
	return p
}

// SetSink sets the pipeline sink
func (p *Pipeline) SetSink(sink Sink) *Pipeline {
	p.sink = sink
	return p
}

// AddFilter adds a filter to the pipeline (filters are ordered)
func (p *Pipeline) AddFilter(filter Filter) *Pipeline {
	p.filters = append(p.filters, filter)
	return p
}

func (p *Pipeline) asyncSource(ctx context.Context, wg *sync.WaitGroup) (<-chan interface{}, <-chan error) {
	stats := &p.sourceStats
	out := make(chan interface{})
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer close(out)
		defer close(errc)
		defer wg.Done()
		for {
			start := time.Now()
			elem, err := p.source.DoWork(ctx)
			elapsed := time.Since(start)
			stats.Mutex.Lock()
			stats.Count++
			stats.Time += elapsed.Nanoseconds()
			stats.Mutex.Unlock()
			
			if err != nil {
				switch err.(type) {
				case NoMoreData:
					return
				default:
					errc <- err
					return
				}
			}
			select {
			case out <- elem:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, errc
}

func (p *Pipeline) asyncFilter(ctx context.Context, filterIndex int, in <-chan interface{}, wg *sync.WaitGroup) (<-chan interface{}, <-chan error) {
	filter := p.filters[filterIndex]
	stats := &p.filtersStats[filterIndex]
	out := make(chan interface{})
	errc := make(chan error, 1)
	var finalErr error
	finalErrMutex := &sync.Mutex{}
	setError := func (newErr error) error {
		finalErrMutex.Lock()
		defer finalErrMutex.Unlock()
		if finalErr == nil {
			finalErr = newErr
		}
		return finalErr
	}
	innerWg := &sync.WaitGroup{}
	innerWg.Add(filter.MaxWorkers())
	for i := 0; i < filter.MaxWorkers(); i++ {
		go func() {
			defer innerWg.Done()
			for elem := range in {
				start := time.Now()
				elem, err := filter.DoWork(ctx, elem)
				elapsed := time.Since(start)
				stats.Mutex.Lock()
				stats.Count++
				stats.Time += elapsed.Nanoseconds()
				stats.Mutex.Unlock()
				err = setError(err)
				if err != nil {
					return
				}
				select {
				case out <- elem:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		innerWg.Wait()
		if finalErr != nil {
			errc <- finalErr
		}
		close(errc)
		close(out)
	}()
	return out, errc
}

func (p *Pipeline) asyncSink(ctx context.Context, in <-chan interface{}, wg *sync.WaitGroup) (<-chan error) {
	stats := &p.sinkStats
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer close(errc)
		defer wg.Done()
		for elem := range in {
			start := time.Now()
			err := p.sink.DoWork(ctx, elem)
			elapsed := time.Since(start)
			stats.Mutex.Lock()
			stats.Count++
			stats.Time += elapsed.Nanoseconds()
			stats.Mutex.Unlock()
			if err != nil {
				errc <- err
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	return errc
}

type closeOnce struct {
	ch		chan struct{}
	once	sync.Once
}

func makeCloseOnce() *closeOnce {
	return &closeOnce{ch: make(chan struct{}, 1)}
}

func (c *closeOnce) close() {
	c.once.Do(func() { close(c.ch) })
}

// StatsCallback is function which can be used to receive statistics from a running pipeline
type StatsCallback func(sourceStats Stats, filtersStats []Stats, sinkStats Stats)

// RunPipeline starts a pipeline and wait until it finished
func (p *Pipeline) RunPipeline(ctx context.Context, statsCb StatsCallback, statsperiod time.Duration) error {
	p.filtersStats = make([]Stats, len(p.filters))
	errorChannels := make([]reflect.SelectCase, len(p.filters)+3) // +3 = source + sink + WaitGroup signaling
	myctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg := &sync.WaitGroup{}
	wgChannel := makeCloseOnce()
	wgChannelIndex := len(p.filters)+2

	out, errc := p.asyncSource(myctx, wg)
	errorChannels[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errc)}
	for filterIndex := range p.filters {
		out, errc = p.asyncFilter(myctx, filterIndex, out, wg)
		errorChannels[filterIndex+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errc)}
	}
	errc = p.asyncSink(myctx, out, wg)
	
	go func() {
		var ticker *time.Ticker
		stopTicker := make(chan struct{}, 1)
		if statsCb != nil {
			ticker = time.NewTicker(statsperiod)
			go func() {
				for {
					select {
					case <-ticker.C:
						sourceStats := p.sourceStats
						filtersStats := make([]Stats, len(p.filtersStats))
						copy(filtersStats, p.filtersStats)
						sinkStats := p.sinkStats
						statsCb(sourceStats, filtersStats, sinkStats)
					case <-stopTicker:
						return
					}
				}
			}()
		}
		wg.Wait()
		wgChannel.close()
		if statsCb != nil {
			ticker.Stop()
			close(stopTicker)
		}
	}()
	
	errorChannels[len(p.filters)+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errc)}
	errorChannels[wgChannelIndex] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(wgChannel.ch)}
	for {
		selectIndex, val, ok := reflect.Select(errorChannels)
		if ok {
			if err, isErr := val.Interface().(error) ; isErr {
				cancel()
				return err
			}
		} else if selectIndex == wgChannelIndex {
			return myctx.Err()
		}
	}
}