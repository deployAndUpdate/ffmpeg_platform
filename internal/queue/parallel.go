package queue

import "context"

// ParallelConsumer supports concurrent job consumption.
type ParallelConsumer interface {
	Consumer
	ConsumeParallel(ctx context.Context, workers int, handler func(ctx context.Context, msg Message) error) error
}
