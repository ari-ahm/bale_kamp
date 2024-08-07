package broker

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"therealbroker/pkg/broker"
)

const subroutinesPerSubject = 8

type subscriber struct {
	msg chan<- *broker.Message
	ctx context.Context
}

type brokerMessageHandler struct {
	subjects sync.Map
}

type subject struct {
	eventChannel chan<- *broker.Message
	subscribers  []sync.Map
	subIdCnt     int
	subLock      sync.Mutex
}

type BrokerMessageHandler interface {
	io.Closer
	addSubscriber(ctx context.Context, subject string, subCtx context.Context) (<-chan *broker.Message, error)
	sendMessage(ctx context.Context, message *broker.Message, subject string) error
}

func NewBrokerMessageHandler() BrokerMessageHandler {
	return &brokerMessageHandler{}
}

func newSubscriber(ctx context.Context, msg chan<- *broker.Message) *subscriber {
	return &subscriber{
		msg: msg,
		ctx: ctx,
	}
}

func newSubject() *subject {
	eventChannel := make(chan *broker.Message, 1000)

	subscribers := make([]sync.Map, subroutinesPerSubject)

	go subjectHandler(context.Background(), eventChannel, subscribers)

	return &subject{
		eventChannel: eventChannel,
		subscribers:  subscribers,
	}
}

func subjectHandler(ctx context.Context, eventChannel <-chan *broker.Message, subs []sync.Map) { // TODO use context
	pubChannels := make([]chan *broker.Message, subroutinesPerSubject)
	for i := 0; i < subroutinesPerSubject; i++ {
		pubChannels[i] = make(chan *broker.Message)
		go func(i int, messageCh <-chan *broker.Message) {
			for msg := range messageCh {
				subs[i].Range(func(k, v interface{}) bool {
					id := k.(int)
					sub := v.(*subscriber)

					select {
					case <-sub.ctx.Done():
						subs[i].Delete(id)
						close(sub.msg)
					case sub.msg <- msg:
					default:
					}

					return true
				})
			}
		}(i, pubChannels[i])
	}

	for msg := range eventChannel {
		for _, sub := range pubChannels {
			sub <- msg
		}
	}
}

func (mh *brokerMessageHandler) addSubscriber(ctx context.Context, subjectId string, subCtx context.Context) (<-chan *broker.Message, error) {
	tmp, ok := mh.subjects.Load(subjectId)
	if !ok {
		tmp = newSubject()
		mh.subjects.Store(subjectId, tmp)
	}

	subject := tmp.(*subject)

	subject.subLock.Lock()
	defer subject.subLock.Unlock()

	subject.subIdCnt++
	id := subject.subIdCnt

	msg := make(chan *broker.Message, 1000)
	subject.subscribers[rand.Intn(subroutinesPerSubject)].Store(id, newSubscriber(subCtx, msg))

	return msg, nil
}

func (mh *brokerMessageHandler) sendMessage(ctx context.Context, message *broker.Message, subjectId string) error {
	tmp, ok := mh.subjects.Load(subjectId)
	if !ok {
		return nil
	}

	subject := tmp.(*subject)

	select {
	case subject.eventChannel <- message:
		return nil
	case <-ctx.Done():
		return broker.ErrContextCanceled
	}
}

func (mh *brokerMessageHandler) Close() error {
	return nil // TODO implement
}
