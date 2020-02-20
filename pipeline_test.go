package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type stupidRandomError struct {
	stage		string
}

func (e stupidRandomError) Error() string {
	return "Stupid Random Error from stage: " + e.stage
}

type numberSource struct {
	start		int
	end			int
	jump		int
	current		int
	randomError	int
}

func makeNumberSource(start, end, jump int) *numberSource {
	return &numberSource{start: start, end: end, jump: jump, current: start}
}

func (src *numberSource) DoWork(ctx context.Context) (interface{}, error) {
	if src.randomError > 0 && rand.Intn(src.randomError) == 0 {
		return nil, stupidRandomError{"source"}
	}
	if src.current < src.end {
		res := src.current
		src.current += src.jump
		return res, nil
	}
	return nil, NoMoreData{}
}

type numberSink struct {
	numberWritten	int
	sum				int
	randomError		int
}

func (sink *numberSink) DoWork(ctx context.Context, input interface{}) error {
	switch v := input.(type) {
	case int:
		if sink.randomError > 0 && rand.Intn(sink.randomError) == 0 {
			return stupidRandomError{"sink"}
		}
		sink.sum += v
		sink.numberWritten++
	default:
		return InvalidInputType{Expected: "int", Actual: fmt.Sprintf("%T", v)}
	}
	return nil
}

type numberAdder struct {
	numberToAdd		int
	workers			int
	randomError		int
}

func (filter *numberAdder) DoWork(ctx context.Context, input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case int:
		if filter.randomError > 0 && rand.Intn(filter.randomError) == 0 {
			return nil, stupidRandomError{"adder"}
		}
		return v + filter.numberToAdd, nil
	default:
		return nil, InvalidInputType{Expected: "int", Actual: fmt.Sprintf("%T", v)}
	}
}

func (filter *numberAdder) MaxWorkers() int {
	return filter.workers
}

type numberMultiplyer struct {
	numberToMultiply		int
	workers					int
	randomError				int
}

func (filter *numberMultiplyer) DoWork(ctx context.Context, input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case int:
		if filter.randomError > 0 && rand.Intn(filter.randomError) == 0 {
			return nil, stupidRandomError{"multiplyer"}
		}
		return v * filter.numberToMultiply, nil
	default:
		return nil, InvalidInputType{Expected: "int", Actual: fmt.Sprintf("%T", v)}
	}
}

func (filter *numberMultiplyer) MaxWorkers() int {
	return filter.workers
}

func TestSimplePipeline(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 10, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 1}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 1}
	stage3 := &numberAdder{numberToAdd: 5, workers: 1}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err != nil {
		t.Errorf("Error returned: %v\n", err)
	}
	if sink.sum != 549 || sink.numberWritten != 9 {
		t.Errorf("Unexpected results: sum = %d, count = %d\n", sink.sum, sink.numberWritten)
	}
}

func TestMultiWorkersPipeline(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 10000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 2}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 5}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err != nil {
		t.Errorf("Error returned: %v\n", err)
	}
	if sink.sum != 350224974 || sink.numberWritten != 9999 {
		t.Errorf("Unexpected results: sum = %d, count = %d\n", sink.sum, sink.numberWritten)
	}
}

func TestStats(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	statsCalled := false
	err := p.RunPipeline(ctx, func(sourceStats Stats, filtersStats []Stats, sinkStats Stats) {
		statsCalled = true
		t.Logf("Source: %v\n", sourceStats)
		t.Logf("Filters: %v\n", filtersStats)
		t.Logf("Sink: %v\n", sinkStats)
	}, time.Millisecond * 300)
	if err != context.DeadlineExceeded {
		t.Errorf("Unexpected error: %v\n", err)
	}
	if !statsCalled {
		t.Error("Stats function wasn't called")
	}
}

func TestTimeout(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := p.RunPipeline(ctx, nil, 0)
	if err != context.DeadlineExceeded {
		t.Errorf("Unexpected error: %v\n", err)
	}
}

func TestCancel(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(2 * time.Second)
		cancel()
	}()
	err := p.RunPipeline(ctx, nil, 0)
	if err != context.Canceled {
		t.Errorf("Unexpected error: %v\n", err)
	}
}

func TestErrorSource(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	source.randomError = 10
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err == nil {
		t.Error("No error raised")
	} else if typedErr, ok := err.(stupidRandomError); !ok || typedErr.stage != "source" {
		t.Errorf("Unexpected error type: %v\n", err)
	}
}

func TestErrorSink(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	sink.randomError = 10
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err == nil {
		t.Error("No error raised")
	} else if typedErr, ok := err.(stupidRandomError); !ok || typedErr.stage != "sink" {
		t.Errorf("Unexpected error type: %v\n", err)
	}
}

func TestErrorFilter(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage1.randomError = 10
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err == nil {
		t.Error("No error raised")
	} else if typedErr, ok := err.(stupidRandomError); !ok || typedErr.stage != "adder" {
		t.Errorf("Unexpected error type: %v\n", err)
	}
}

func TestErrorSecondFilter(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage2.randomError = 10
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err == nil {
		t.Error("No error raised")
	} else if typedErr, ok := err.(stupidRandomError); !ok || typedErr.stage != "multiplyer" {
		t.Errorf("Unexpected error type: %v\n", err)
	}
}

func TestErrorLastFilter(t *testing.T) {
	p := MakePipeline()
	source := makeNumberSource(1, 1000000, 1)
	stage1 := &numberAdder{numberToAdd: 3, workers: 4}
	stage2 := &numberMultiplyer{numberToMultiply: 7, workers: 2}
	stage3 := &numberAdder{numberToAdd: 5, workers: 3}
	stage3.randomError = 10
	sink := &numberSink{}
	p.SetSource(source).AddFilter(stage1).AddFilter(stage2).AddFilter(stage3).SetSink(sink)
	err := p.RunPipeline(context.Background(), nil, 0)
	if err == nil {
		t.Error("No error raised")
	} else if typedErr, ok := err.(stupidRandomError); !ok || typedErr.stage != "adder" {
		t.Errorf("Unexpected error type: %v\n", err)
	}
}