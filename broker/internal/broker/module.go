package broker

import (
	"context"
	"github.com/madflojo/tasks"
	"log"
	"math/rand"
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
	msg chan broker.Message
}

type subjectStruct struct {
	msgIdCnt      atomic.Int64
	subIdCnt      int
	messages      sync.Map
	subscribers   []sync.Map
	pubLock       sync.Mutex
	subLock       sync.Mutex
	messageEvents chan<- *broker.Message
}

func NewModule() broker.Broker {
	return &Module{
		scheduler: tasks.New(),
	}
}

func newSubscriber(ctx context.Context) *subscriber {
	msg := make(chan broker.Message, 1000) // TODO read from env
	return &subscriber{
		msg: msg,
		ctx: ctx,
	}
}

func newSubjectStruct() *subjectStruct {
	eventChannel := make(chan *broker.Message, 1000) // TODO read 1000 from env
	subRoutinesCnt := 8                              // TODO read from env
	subj := subjectStruct{
		messageEvents: eventChannel,
		subscribers:   make([]sync.Map, subRoutinesCnt),
	}

	go func() { // TODO add mainCtx
		subChannels := make([]chan *broker.Message, subRoutinesCnt)
		for i := 0; i < subRoutinesCnt; i++ {
			subChannels[i] = make(chan *broker.Message)
			go func(i int, ch <-chan *broker.Message) {
				for msg := range ch {
					subj.subscribers[i].Range(func(k, v interface{}) bool {
						id := k.(int)
						sub := v.(*subscriber)

						select {
						case <-sub.ctx.Done():
							subj.subscribers[i].Delete(id)
							close(sub.msg)
						case sub.msg <- *msg:
						default:
						}

						return true
					})
				}
			}(i, subChannels[i])
		}

		for msg := range eventChannel {
			for _, sub := range subChannels {
				sub <- msg
			}
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

func (m *Module) Close() error { // TODO clean
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
		for i, _ := range v.(*subjectStruct).subscribers {
			v.(*subjectStruct).subscribers[i].Range(func(k, v interface{}) bool {
				close(v.(chan<- broker.Message))
				return true
			})
		}
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

	tmp, ok := m.subjects.Load(subject)
	if !ok {
		tmp = newSubjectStruct()
		m.subjects.Store(subject, tmp)
	}
	subj := tmp.(*subjectStruct)

	subj.pubLock.Lock() // TODO remove when adding db
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

	tmp, ok := m.subjects.Load(subject)
	if !ok {
		tmp = newSubjectStruct()
		m.subjects.Store(subject, tmp)
	}
	subj := tmp.(*subjectStruct)

	subj.subLock.Lock() // TODO remove when adding db
	defer subj.subLock.Unlock()

	subj.subIdCnt++
	newSub := newSubscriber(context.WithoutCancel(ctx))
	subj.subscribers[rand.Intn(len(subj.subscribers))].Store(subj.subIdCnt, newSub)

	return newSub.msg, nil
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
