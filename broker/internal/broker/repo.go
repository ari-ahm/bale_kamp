package broker

import (
	"context"
	"github.com/madflojo/tasks"
	"io"
	"log"
	"sync"
	"therealbroker/pkg/broker"
)

type brokerRepo struct {
	scheduler *tasks.Scheduler
	msgIdCnt  int
	messages  map[string]map[int]*broker.Message
	lock      sync.Mutex
}

type BrokerRepo interface {
	io.Closer
	save(ctx context.Context, message *broker.Message, subject string) (id int, err error)
	load(ctx context.Context, id int, subject string) (message *broker.Message, err error)
}

func NewBrokerRepo() BrokerRepo {
	return &brokerRepo{
		scheduler: tasks.New(),
		messages:  make(map[string]map[int]*broker.Message),
	}
}

func (r *brokerRepo) save(ctx context.Context, message *broker.Message, subject string) (int, error) {
	r.lock.Lock() // TODO change
	defer r.lock.Unlock()

	if _, ok := r.messages[subject]; !ok {
		r.messages[subject] = make(map[int]*broker.Message)
	}

	id := 0
	if message.Expiration != 0 {
		r.msgIdCnt++
		id = r.msgIdCnt
		message.Id = id
		r.messages[subject][id] = message
		_, err := r.scheduler.Add(&tasks.Task{
			Interval: message.Expiration,
			RunOnce:  true,
			TaskFunc: func() error {
				r.lock.Lock()
				defer r.lock.Unlock()

				delete(r.messages[subject], id)
				return nil
			},
		})

		if err != nil {
			log.Println("Failed to add task:", err)
			return 0, broker.ErrInternalError
		}
	}

	return id, nil
}

func (r *brokerRepo) load(ctx context.Context, id int, subject string) (*broker.Message, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.messages[subject]; !ok {
		r.messages[subject] = make(map[int]*broker.Message)
	}

	if id < 1 || id > r.msgIdCnt {
		return nil, broker.ErrInvalidID
	}

	msg, ok := r.messages[subject][id]
	if !ok {
		return nil, broker.ErrExpiredID
	}

	return msg, nil
}

func (r *brokerRepo) Close() error {
	r.scheduler.Stop()
	return nil
}
