package broker

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
	"therealbroker/pkg/broker"
	"time"
)

var (
	dbLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "broker_db_latency",
		Buckets: []float64{10, 20, 35, 50, 75, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000, 4500, 5000},
	})
	deliverLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "broker_deliver_latency",
		Buckets: []float64{10, 20, 35, 50, 75, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000, 4500, 5000},
	})
)

type module struct {
	ctx            context.Context
	close          context.CancelFunc
	wg             sync.WaitGroup
	repo           BrokerRepo
	messageHandler BrokerMessageHandler
}

func NewModule(repo BrokerRepo, messageHandler BrokerMessageHandler) broker.Broker {
	ctx, cancel := context.WithCancel(context.Background())
	return &module{
		repo:           repo,
		messageHandler: messageHandler,
		ctx:            ctx,
		close:          cancel,
	}
}

func preChecks(m *module) error {
	if m == nil {
		return broker.ErrNilPointer
	}

	select {
	case <-m.ctx.Done():
		return broker.ErrUnavailable
	default:
	}

	return nil
}

func (m *module) Close() error {
	if err := preChecks(m); err != nil {
		return err
	}

	m.close()
	m.wg.Wait()

	if err := m.messageHandler.Close(); err != nil {
		return err
	}

	if err := m.repo.Close(); err != nil {
		return err
	}

	return nil
}

func (m *module) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return 0, err
	}

	tm := time.Now()
	err := m.messageHandler.sendMessage(ctx, &msg, subject)
	if err != nil {
		return 0, err
	}
	deliverLatency.Observe(float64(time.Since(tm).Milliseconds()))

	tm = time.Now()
	id, err := m.repo.save(ctx, &msg, subject)
	if err != nil {
		return 0, err
	}
	dbLatency.Observe(float64(time.Since(tm).Milliseconds()))

	return id, nil
}

func (m *module) Subscribe(ctx context.Context, subject string) (<-chan *broker.Message, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return nil, err
	}

	msg, err := m.messageHandler.addSubscriber(ctx, subject, ctx)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *module) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return broker.Message{}, err
	}

	msg, err := m.repo.load(ctx, id, subject)
	if err != nil {
		return broker.Message{}, err
	}

	return *msg, nil
}
