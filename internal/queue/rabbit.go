package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName   = "jobs.direct"
	queueMain      = "jobs.main"
	queueRetry     = "jobs.retry"
	queueDLQ       = "jobs.dlq"
	routingMain    = "dispatch"
	routingRetry   = "retry"
	routingDLQ     = "dlq"
	defaultTimeout = 5 * time.Second
)

// RabbitConfig holds RabbitMQ connection settings.
type RabbitConfig struct {
	URL              string
	PublishTimeout   time.Duration
	Prefetch         int
	RetryDelayMillis int
}

// Rabbit implements Publisher and Consumer over AMQP.
type Rabbit struct {
	cfg        RabbitConfig
	conn       *amqp.Connection
	ch         *amqp.Channel
	declareOnce sync.Once
	declareErr  error
}

// NewRabbit connects and declares topology.
func NewRabbit(cfg RabbitConfig) (*Rabbit, error) {
	return NewRabbitWithRetry(cfg, 1, 0)
}

// NewRabbitWithRetry dials RabbitMQ with retries (for container startup races).
func NewRabbitWithRetry(cfg RabbitConfig, attempts int, pause time.Duration) (*Rabbit, error) {
	if attempts <= 0 {
		attempts = 1
	}
	if pause <= 0 {
		pause = 500 * time.Millisecond
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		r, err := newRabbitOnce(cfg)
		if err == nil {
			return r, nil
		}
		lastErr = err
		if i+1 < attempts {
			time.Sleep(pause)
		}
	}
	return nil, lastErr
}

func newRabbitOnce(cfg RabbitConfig) (*Rabbit, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("rabbit URL is required")
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = defaultTimeout
	}
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 1
	}
	if cfg.RetryDelayMillis <= 0 {
		cfg.RetryDelayMillis = 30000
	}

	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbit: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	r := &Rabbit{cfg: cfg, conn: conn, ch: ch}
	if err := r.ensureTopology(); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

// ConfigFromEnv reads RABBITMQ_URL and optional tuning env vars.
func ConfigFromEnv() (RabbitConfig, bool) {
	url := getenv("RABBITMQ_URL", "")
	if url == "" {
		return RabbitConfig{}, false
	}
	prefetch := getenvInt("RABBITMQ_PREFETCH", 1)
	retryDelay := getenvInt("RABBITMQ_RETRY_DELAY_MS", 30000)
	timeout := getenvDuration("RABBITMQ_PUBLISH_TIMEOUT", defaultTimeout)
	return RabbitConfig{
		URL:              url,
		PublishTimeout:   timeout,
		Prefetch:         prefetch,
		RetryDelayMillis: retryDelay,
	}, true
}

func (r *Rabbit) ensureTopology() error {
	r.declareOnce.Do(func() {
		r.declareErr = r.declareTopologyOnce()
	})
	return r.declareErr
}

func (r *Rabbit) declareTopologyOnce() error {
	if err := r.ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	dlqArgs := amqp.Table{}
	if _, err := r.ch.QueueDeclare(queueDLQ, true, false, false, false, dlqArgs); err != nil {
		return fmt.Errorf("declare dlq: %w", err)
	}
	if err := r.ch.QueueBind(queueDLQ, routingDLQ, exchangeName, false, nil); err != nil {
		return fmt.Errorf("bind dlq: %w", err)
	}

	mainArgs := amqp.Table{
		"x-dead-letter-exchange": exchangeName,
		"x-dead-letter-routing-key": routingDLQ,
	}
	if _, err := r.ch.QueueDeclare(queueMain, true, false, false, false, mainArgs); err != nil {
		return fmt.Errorf("declare main queue: %w", err)
	}
	if err := r.ch.QueueBind(queueMain, routingMain, exchangeName, false, nil); err != nil {
		return fmt.Errorf("bind main queue: %w", err)
	}

	retryArgs := amqp.Table{
		"x-message-ttl":             int32(r.cfg.RetryDelayMillis),
		"x-dead-letter-exchange":    exchangeName,
		"x-dead-letter-routing-key": routingMain,
	}
	if _, err := r.ch.QueueDeclare(queueRetry, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}
	if err := r.ch.QueueBind(queueRetry, routingRetry, exchangeName, false, nil); err != nil {
		return fmt.Errorf("bind retry queue: %w", err)
	}

	return nil
}

// Ping verifies the broker connection is alive.
func (r *Rabbit) Ping(ctx context.Context) error {
	if err := r.ensureTopology(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if r.conn.IsClosed() {
		return fmt.Errorf("rabbit connection closed")
	}
	return nil
}

// Publish sends a message with publisher confirms.
func (r *Rabbit) Publish(ctx context.Context, target Target, msg Message) error {
	if err := r.ensureTopology(); err != nil {
		return err
	}

	body, err := EncodeMessage(msg)
	if err != nil {
		return err
	}

	routingKey, err := routingForTarget(target)
	if err != nil {
		return err
	}

	pubCtx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()

	if err := r.ch.PublishWithContext(pubCtx, exchangeName, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

// Consume reads from jobs.main with manual ack (one message at a time).
func (r *Rabbit) Consume(ctx context.Context, handler func(ctx context.Context, msg Message) error) error {
	return r.consume(ctx, r.cfg.Prefetch, handler)
}

// ConsumeParallel processes up to workers messages concurrently with manual ack after handler success.
func (r *Rabbit) ConsumeParallel(ctx context.Context, workers int, handler func(ctx context.Context, msg Message) error) error {
	if workers <= 0 {
		workers = 1
	}
	return r.consumeParallel(ctx, workers, handler)
}

func (r *Rabbit) consume(ctx context.Context, prefetch int, handler func(ctx context.Context, msg Message) error) error {
	if err := r.ensureTopology(); err != nil {
		return err
	}
	if err := r.ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := r.ch.Consume(queueMain, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			if err := r.handleDelivery(ctx, d, handler); err != nil {
				_ = d.Nack(false, false)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

func (r *Rabbit) consumeParallel(ctx context.Context, workers int, handler func(ctx context.Context, msg Message) error) error {
	if err := r.ensureTopology(); err != nil {
		return err
	}
	if err := r.ch.Qos(workers, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := r.ch.Consume(queueMain, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	slots := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				wg.Wait()
				return fmt.Errorf("delivery channel closed")
			}
			slots <- struct{}{}
			wg.Add(1)
			go func(d amqp.Delivery) {
				defer wg.Done()
				defer func() { <-slots }()
				if err := r.handleDelivery(ctx, d, handler); err != nil {
					_ = d.Nack(false, false)
					return
				}
				_ = d.Ack(false)
			}(d)
		}
	}
}

func (r *Rabbit) handleDelivery(ctx context.Context, d amqp.Delivery, handler func(ctx context.Context, msg Message) error) error {
	msg, err := DecodeMessage(d.Body)
	if err != nil {
		return err
	}
	return handler(ctx, msg)
}

// PurgeQueues removes all messages from main, retry, and dlq (for tests).
func (r *Rabbit) PurgeQueues() error {
	if err := r.ensureTopology(); err != nil {
		return err
	}
	for _, q := range []string{queueMain, queueRetry, queueDLQ} {
		if _, err := r.ch.QueuePurge(q, false); err != nil {
			return fmt.Errorf("purge %s: %w", q, err)
		}
	}
	return nil
}

// MessagesReady returns combined ready message count on main and retry queues.
func (r *Rabbit) MessagesReady() (int, error) {
	if err := r.ensureTopology(); err != nil {
		return 0, err
	}
	total := 0
	for _, q := range []string{queueMain, queueRetry} {
		info, err := r.ch.QueueInspect(q)
		if err != nil {
			return 0, err
		}
		total += info.Messages
	}
	return total, nil
}

// Close shuts down channel and connection.
func (r *Rabbit) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func routingForTarget(target Target) (string, error) {
	switch target {
	case TargetMain:
		return routingMain, nil
	case TargetRetry:
		return routingRetry, nil
	default:
		return "", fmt.Errorf("unknown queue target %q", target)
	}
}

func getenv(key, fallback string) string {
	if v := getenvRaw(key); v != "" {
		return v
	}
	return fallback
}

func getenvRaw(key string) string {
	// avoid importing os in multiple files — use small helper in env.go
	return lookupEnv(key)
}

func getenvInt(key string, fallback int) int {
	v := lookupEnv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return fallback
	}
	return n
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := lookupEnv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
