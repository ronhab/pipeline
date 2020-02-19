package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// NoMoreData is the "error" a source doWork() method should return when it doesn't have any more data
type NoMoreData struct {
}

func (e NoMoreData) Error() string {
	return "Not an error: no more data"
}

// InvalidInputType is the error a pipeline element doWork() method should return when it receive data with the wrong type
type InvalidInputType struct {
	Expected	string
	Actual		string
}

func (e InvalidInputType) Error() string {
	return fmt.Sprintf("Invalid input format: expected = %s, actual = %s", e.Expected, e.Actual)
}

// Source represents an element on the pipeline which has no input
type Source interface {
	doWork(ctx context.Context) (interface{}, error)
}

// Filter represents an element on the pipeline which has both input and output
type Filter interface {
	maxWorkers() int
	doWork(ctx context.Context, input interface{}) (interface{}, error)
}

// Sink represents an element on  the pipeline which has no output
type Sink interface {
	doWork(ctx context.Context, input interface{}) error
}

// Pipeline represents a pipeline object
type Pipeline struct {
	source			Source
	filters			[]Filter
	sink			Sink
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
	out := make(chan interface{})
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer close(out)
		defer close(errc)
		defer wg.Done()
		for {
			elem, err := p.source.doWork(ctx)
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
	innerWg.Add(filter.maxWorkers())
	for i := 0; i < filter.maxWorkers(); i++ {
		go func() {
			defer innerWg.Done()
			for elem := range in {
				elem, err := filter.doWork(ctx, elem)
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
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer close(errc)
		defer wg.Done()
		for elem := range in {
			err := p.sink.doWork(ctx, elem)
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

// RunPipeline starts a pipeline and wait until it finished
func (p *Pipeline) RunPipeline(ctx context.Context) error {
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
		wg.Wait()
		wgChannel.close()
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