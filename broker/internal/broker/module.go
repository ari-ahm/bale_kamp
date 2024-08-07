package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"therealbroker/pkg/broker"
)

type module struct {
	closing        atomic.Bool
	wg             sync.WaitGroup
	repo           BrokerRepo
	messageHandler BrokerMessageHandler
}

func NewModule(repo BrokerRepo, messageHandler BrokerMessageHandler) broker.Broker {
	return &module{
		repo:           repo,
		messageHandler: messageHandler,
	}
}

func preChecks(m *module) error {
	if m == nil {
		return broker.ErrNilPointer
	}

	if m.closing.Load() {
		return broker.ErrUnavailable
	}

	return nil
}

func (m *module) Close() error { // TODO clean
	if err := preChecks(m); err != nil {
		return err
	}

	oldClosing := m.closing.Swap(true)
	if oldClosing {
		return broker.ErrUnavailable
	}

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

	id, err := m.repo.save(ctx, &msg, subject)
	if err != nil {
		return 0, err
	}

	if err := m.messageHandler.sendMessage(ctx, &msg, subject); err != nil {
		return 0, err
	}

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
