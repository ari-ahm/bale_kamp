package broker

import (
	"context"
	"github.com/madflojo/tasks"
	"log"
	"sync"
	"sync/atomic"
	"therealbroker/pkg/broker"
)

type Module struct {
	subjects  sync.Map
	closing   atomic.Bool
	scheduler *tasks.Scheduler
	wg        sync.WaitGroup
}

type subscriber struct {
	ctx context.Context
	msg chan<- broker.Message
}

type subjectStruct struct {
	msgIdCnt      atomic.Int64
	subIdCnt      int
	messages      sync.Map
	subscribers   sync.Map
	pubLock       sync.Mutex
	subLock       sync.Mutex
	messageEvents chan<- *broker.Message
}

func NewModule() broker.Broker {
	return &Module{
		scheduler: tasks.New(),
	}
}

func newSubscriber(ctx context.Context, msg chan<- broker.Message) *subscriber {
	return &subscriber{
		msg: msg,
		ctx: ctx,
	}
}

func newSubjectStruct() *subjectStruct {
	ch := make(chan *broker.Message, 1000) // TODO read 1000 from env
	subj := subjectStruct{
		messageEvents: ch,
	}

	go func() {
		for msg := range ch {
			subj.subscribers.Range(func(k, v interface{}) bool {
				key := k.(int)
				value := v.(*subscriber)

				select {
				case <-value.ctx.Done():
					subj.subscribers.Delete(key)
					close(value.msg)
				case value.msg <- *msg:
				default:
				}

				return true
			})
		}
	}()

	return &subj
}

func preChecks(m *Module) error {
	if m == nil {
		return broker.ErrNilPointer
	}

	if m.closing.Load() {
		return broker.ErrUnavailable
	}

	return nil
}

func (m *Module) Close() error {
	if err := preChecks(m); err != nil {
		return err
	}

	oldClosing := m.closing.Swap(true)
	if oldClosing {
		return broker.ErrUnavailable
	}

	m.wg.Wait()
	m.scheduler.Stop()

	m.subjects.Range(func(k, v interface{}) bool {
		close(v.(*subjectStruct).messageEvents)
		v.(*subjectStruct).subscribers.Range(func(k, v interface{}) bool {
			close(v.(chan<- broker.Message))
			return true
		})
		return true
	})

	return nil
}

func (m *Module) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return 0, err
	}
	tmp, _ := m.subjects.LoadOrStore(subject, newSubjectStruct())
	subj := tmp.(*subjectStruct)

	subj.pubLock.Lock()
	defer subj.pubLock.Unlock()

	if msg.Expiration != 0 {
		id := int(subj.msgIdCnt.Add(1))
		msg.Id = id
		subj.messages.Store(id, &msg)
		_, err := m.scheduler.Add(&tasks.Task{
			Interval: msg.Expiration,
			RunOnce:  true,
			TaskFunc: func() error {
				subj.messages.Delete(id)
				return nil
			},
		})

		if err != nil {
			log.Println("Failed to add task:", err)
			return 0, broker.ErrInternalError
		}
	}

	select {
	case subj.messageEvents <- &msg:
	case <-ctx.Done():
		return 0, broker.ErrContextCanceled
	}

	return int(subj.msgIdCnt.Load()), nil
}

func (m *Module) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return nil, err
	}
	tmp, _ := m.subjects.LoadOrStore(subject, newSubjectStruct())
	subj := tmp.(*subjectStruct)

	subj.subLock.Lock()
	defer subj.subLock.Unlock()

	subj.subIdCnt++
	msg := make(chan broker.Message, 1000)
	subj.subscribers.Store(subj.subIdCnt, newSubscriber(context.WithoutCancel(ctx), msg))

	return msg, nil
}

func (m *Module) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
	m.wg.Add(1)
	defer m.wg.Done()

	if err := preChecks(m); err != nil {
		return broker.Message{}, err
	}

	tmp, ok := m.subjects.Load(subject)
	if !ok {
		return broker.Message{}, broker.ErrInvalidID
	}
	subj := tmp.(*subjectStruct)

	if id < 1 || id > int(subj.msgIdCnt.Load()) {
		return broker.Message{}, broker.ErrInvalidID
	}

	msg, ok := subj.messages.Load(id)

	if !ok {
		return broker.Message{}, broker.ErrExpiredID
	}

	return *msg.(*broker.Message), nil
}
