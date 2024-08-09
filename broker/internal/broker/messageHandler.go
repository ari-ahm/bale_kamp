package broker

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"therealbroker/pkg/broker"
)

var (
	subroutinesPerSubject = func() int {
		ret, err := strconv.Atoi(os.Getenv("SUBROUTINES_PER_SUBJECT"))
		if err != nil {
			panic(err)
		}
		return ret
	}()

	messageQueueLenMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_message_queue_len",
		Help: "message queue length",
	})
)

type subscriber struct {
	msg chan<- *broker.Message
	ctx context.Context
}

type brokerMessageHandler struct {
	subjects sync.Map
	ctx      context.Context
	close    context.CancelFunc
}

type subject struct {
	messageQueue     []*broker.Message
	messageQueueCond *sync.Cond
	subscribers      []sync.Map
	subIdCnt         int
	subLock          sync.Mutex
	ctx              context.Context
}

type BrokerMessageHandler interface {
	io.Closer
	addSubscriber(ctx context.Context, subject string, subCtx context.Context) (<-chan *broker.Message, error)
	sendMessage(ctx context.Context, message *broker.Message, subject string) error
}

func NewBrokerMessageHandler() BrokerMessageHandler {
	ctx, cancel := context.WithCancel(context.Background())
	return &brokerMessageHandler{
		ctx:   ctx,
		close: cancel,
	}
}

func newSubscriber(ctx context.Context, msg chan<- *broker.Message) *subscriber {
	return &subscriber{
		msg: msg,
		ctx: ctx,
	}
}

func newSubject(ctx context.Context) *subject {
	subscribers := make([]sync.Map, subroutinesPerSubject)

	subCtx := context.WithoutCancel(ctx)
	messageQueueCond := sync.NewCond(&sync.Mutex{})

	ret := &subject{
		subscribers:      subscribers,
		ctx:              subCtx,
		messageQueue:     make([]*broker.Message, 0),
		messageQueueCond: messageQueueCond,
	}
	go subjectHandler(subCtx, subscribers, &ret.messageQueue, messageQueueCond)

	return ret
}

func subjectHandler(ctx context.Context, subs []sync.Map, messageQueue *[]*broker.Message, messageQueueCond *sync.Cond) {
	pubChannels := make([]chan *broker.Message, subroutinesPerSubject)
	for i := 0; i < subroutinesPerSubject; i++ {
		pubChannels[i] = make(chan *broker.Message)
		go func(i int, messageCh <-chan *broker.Message) {
			for {
				select {
				case msg := <-messageCh:
					subs[i].Range(func(k, v interface{}) bool {
						id := k.(int)
						sub := v.(*subscriber)

						select {
						case <-sub.ctx.Done():
							subs[i].Delete(id)
							close(sub.msg)
						case sub.msg <- msg:
						case <-ctx.Done():
							return false
						default:
						}

						return true
					})
				case <-ctx.Done():
					return
				}
			}
		}(i, pubChannels[i])
	}

	for {
		messageQueueCond.L.Lock()
		for len(*messageQueue) == 0 {
			messageQueueCond.Wait()
		}
		message := (*messageQueue)[0]
		*messageQueue = (*messageQueue)[1:]
		messageQueueLenMetric.Set(float64(len(*messageQueue)))
		messageQueueCond.L.Unlock()

		for _, sub := range pubChannels {
			sub <- message
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (mh *brokerMessageHandler) addSubscriber(ctx context.Context, subjectId string, subCtx context.Context) (<-chan *broker.Message, error) {
	tmp, ok := mh.subjects.Load(subjectId)
	if !ok {
		tmp = newSubject(mh.ctx)
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

	subject.messageQueueCond.L.Lock()
	subject.messageQueue = append(subject.messageQueue, message)
	subject.messageQueueCond.L.Unlock()
	subject.messageQueueCond.Signal()

	return nil
}

func (mh *brokerMessageHandler) Close() error {
	mh.close()
	return nil
}
