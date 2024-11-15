package broker

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"sync"
	"testing"
	"therealbroker/pkg/broker"
	"time"
)

var (
	service broker.Broker
	mainCtx = context.Background()
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
	m.Run()
}

func TestPublishShouldFailOnClosed(t *testing.T) {
	msg := createMessage()

	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Publish(mainCtx, "ali", *msg)
	assert.Equal(t, broker.ErrUnavailable, err)

	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
}

func TestSubscribeShouldFailOnClosed(t *testing.T) {
	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Subscribe(mainCtx, "ali")
	assert.Equal(t, broker.ErrUnavailable, err)

	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
}

func TestFetchShouldFailOnClosed(t *testing.T) {
	err := service.Close()
	assert.Nil(t, err)

	_, err = service.Fetch(mainCtx, "ali", randomString(10))
	assert.Equal(t, broker.ErrUnavailable, err)

	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
}

func TestPublishShouldNotFail(t *testing.T) {
	msg := createMessage()

	_, err := service.Publish(mainCtx, "ali", *msg)

	assert.Equal(t, nil, err)
}

func TestSubscribeShouldNotFail(t *testing.T) {
	sub, err := service.Subscribe(mainCtx, "ali")

	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, sub)
}

func TestPublishShouldSendMessageToSubscribedChan(t *testing.T) {
	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
	msg := createMessage()

	sub, _ := service.Subscribe(mainCtx, "ali")
	id, _ := service.Publish(mainCtx, "ali", *msg)
	in := <-sub
	msg.Id = id

	assert.Equal(t, msg, in)
}

func TestPublishShouldSendMessageToSubscribedChans(t *testing.T) {
	msg := createMessage()

	sub1, _ := service.Subscribe(mainCtx, "ali")
	sub2, _ := service.Subscribe(mainCtx, "ali")
	sub3, _ := service.Subscribe(mainCtx, "ali")
	_, _ = service.Publish(mainCtx, "ali", *msg)
	in1 := <-sub1
	in2 := <-sub2
	in3 := <-sub3

	assert.Equal(t, msg.Body, in1.Body)
	assert.Equal(t, msg.Body, in2.Body)
	assert.Equal(t, msg.Body, in3.Body)
}

func TestPublishShouldPreserveOrder(t *testing.T) {
	n := 50
	messages := make([]*broker.Message, n)
	sub, _ := service.Subscribe(mainCtx, "ali")
	for i := 0; i < n; i++ {
		messages[i] = createMessage()
		_, _ = service.Publish(mainCtx, "ali", *messages[i])
	}

	for i := 0; i < n; i++ {
		msg := <-sub
		assert.Equal(t, messages[i].Body, msg.Body)
	}
}

func TestPublishShouldNotSendToOtherSubscriptions(t *testing.T) {
	msg := createMessage()
	ali, _ := service.Subscribe(mainCtx, "ali")
	maryam, _ := service.Subscribe(mainCtx, "maryam")

	_, _ = service.Publish(mainCtx, "ali", *msg)
	select {
	case m := <-ali:
		assert.Equal(t, msg.Body, m.Body)
	case <-maryam:
		assert.Fail(t, "Wrong message received")
	}
}

func TestNonExpiredMessageShouldBeFetchable(t *testing.T) {
	msg := createMessageWithExpire(time.Second * 10)
	id, _ := service.Publish(mainCtx, "ali", *msg)
	msg.Id = id
	msg.Expiration = 0
	fMsg, _ := service.Fetch(mainCtx, "ali", id)

	assert.Equal(t, *msg, fMsg)
}

func TestExpiredMessageShouldNotBeFetchable(t *testing.T) {
	msg := createMessageWithExpire(time.Millisecond * 500)
	id, _ := service.Publish(mainCtx, "ali", *msg)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	<-ticker.C
	fMsg, _ := service.Fetch(mainCtx, "ali", id)
	//assert.Equal(t, broker.ErrExpiredID, err) // TODO
	assert.Equal(t, broker.Message{}, fMsg)
}

func TestNewSubscriptionShouldNotGetPreviousMessages(t *testing.T) {
	msg := createMessage()
	_, _ = service.Publish(mainCtx, "ali", *msg)
	sub, _ := service.Subscribe(mainCtx, "ali")

	select {
	case <-sub:
		assert.Fail(t, "Got previous message")
	default:
	}
}

func TestConcurrentSubscribesOnOneSubjectShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Subscribe(mainCtx, "ali")
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentSubscribesShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(2000 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Subscribe(mainCtx, randomString(4))
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishOnOneSubjectShouldNotFail(t *testing.T) {
	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				ctx, _ := context.WithDeadline(mainCtx, time.Now().Add(10*time.Second))
				_, err := service.Publish(ctx, "ali", *msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestConcurrentPublishShouldNotFail(t *testing.T) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	msg := createMessage()

	for {
		select {
		case <-ticker.C:
			wg.Wait()
			return

		default:
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := service.Publish(mainCtx, randomString(4), *msg)
				assert.Nil(t, err)
			}()
		}
	}
}

func TestDataRace(t *testing.T) {
	service := NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
	duration := 10000 * time.Millisecond
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var wg sync.WaitGroup

	ids := make(chan string, 100000)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				id, err := service.Publish(mainCtx, "ali", *createMessageWithExpire(duration))
				ids <- id
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			default:
				_, err := service.Subscribe(mainCtx, "ali")
				assert.Nil(t, err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				return

			case id := <-ids:
				_, err := service.Fetch(mainCtx, "ali", id)
				assert.Nil(t, err)
			}
		}
	}()

	wg.Wait()
}

func BenchmarkPublish(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Publish(mainCtx, randomString(2), *createMessage())
		assert.Nil(b, err)
	}
}

func BenchmarkSubscribe(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Subscribe(mainCtx, randomString(2))
		assert.Nil(b, err)
	}
}

func BenchmarkPublish2(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Publish(mainCtx, randomString(2), *createMessage())
		assert.Nil(b, err)
	}
}

func BenchmarkSubscribe2(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := service.Subscribe(mainCtx, randomString(2))
		assert.Nil(b, err)
	}
}

func TestAsli(t *testing.T) {
	service = NewModule(NewBrokerRepo(), NewBrokerMessageHandler())
	cnt1 := 1
	cnt2 := 10000

	finalTrue := 0
	finalTrueLock := sync.Mutex{}

	mmdwg := sync.WaitGroup{}
	tm := time.Now()
	for i := 0; i < cnt1; i++ {
		ch, err := service.Subscribe(mainCtx, "ali")
		assert.Nil(t, err)
		mmdwg.Add(1)
		go func() {
			defer mmdwg.Done()
			tmp := 0
			for i := range ch {
				if i.Body == "mammad" {
					tmp++
				}
				if tmp == cnt2 {
					finalTrueLock.Lock()
					finalTrue++
					finalTrueLock.Unlock()
					return
				}
			}
		}()
	}
	dur := time.Since(tm)
	log.Println(dur.Milliseconds(), int64(dur.Nanoseconds())/int64(cnt1))

	time.Sleep(2 * time.Second)

	wg := sync.WaitGroup{}
	tm = time.Now()
	for i := 0; i < cnt2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.Publish(mainCtx, "ali", broker.Message{Body: "mammad"})
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
	dur = time.Since(tm)
	log.Println(dur.Milliseconds(), int64(dur.Nanoseconds())/int64(cnt2))
	mmdwg.Wait()
	log.Println(finalTrue)
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func createMessage() *broker.Message {
	body := randomString(16)

	return &broker.Message{
		Body:       body,
		Expiration: 0,
	}
}

func createMessageWithExpire(duration time.Duration) *broker.Message {
	body := randomString(16)

	return &broker.Message{
		Body:       body,
		Expiration: duration,
	}
}
