package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	"io"
	"log"
	"math/rand"
	"sync"
	"therealbroker/api/proto"
	"therealbroker/pkg/broker"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type brokerSpy struct {
	client proto.BrokerClient
}

func (b brokerSpy) Close() error {
	return nil
}

func (b brokerSpy) Publish(ctx context.Context, subject string, msg broker.Message) (string, error) {
	res, err := b.client.Publish(context.WithoutCancel(ctx), &proto.PublishRequest{
		Subject:           subject,
		Body:              []byte(msg.Body),
		ExpirationSeconds: int32((msg.Expiration + time.Second - 1) / time.Second),
	})

	return res.GetId(), err
}

func (b brokerSpy) Subscribe(ctx context.Context, subject string) (<-chan *broker.Message, error) {
	res, err := b.client.Subscribe(context.WithoutCancel(ctx), &proto.SubscribeRequest{
		Subject: subject,
	})

	ch := make(chan *broker.Message)

	go func() {
		defer close(ch)

		for {
			in, err := res.Recv()
			if err != nil {
				if err != io.EOF {
					log.Println("recv error:", err)
				}
				return
			}

			ch <- &broker.Message{
				Body: string(in.GetBody()),
			}
		}
	}()

	return ch, err
}

func (b brokerSpy) Fetch(ctx context.Context, subject string, id string) (broker.Message, error) {
	res, err := b.client.Fetch(context.WithoutCancel(ctx), &proto.FetchRequest{
		Subject: subject,
		Id:      id,
	})

	return broker.Message{
		Body: string(res.GetBody()),
	}, err
}

func newSpyModule() broker.Broker {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	client := proto.NewBrokerClient(conn)

	return &brokerSpy{client: client}
}

func main() {
	bud := newSpyModule()
	subCntFlag := flag.Int("subCnt", 10, "number of subscriptions")
	pubCntFlag := flag.Int("pubCnt", 10, "number of publishes")
	deadline := flag.Int("deadline", 100, "deadline seconds")

	flag.Parse()

	subCnt := *subCntFlag
	pubCnt := *pubCntFlag

	jafar := 0

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(*deadline)*time.Second)
	actSubCnt := 0
	subFinishCnt := 0
	subFinishCntLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	tm := time.Now()
	for i := 0; i < subCnt; i++ {
		wg.Add(1)
		go func() {
			ch, _ := bud.Subscribe(ctx, "randomString(3)")
			wg.Done()
			cnt := 0
			for i := range ch {
				if i.Body != "" {
					cnt++
					jafar++
				}
				if cnt == pubCnt {
					break
				}
			}
			subFinishCntLock.Lock()
			subFinishCnt++
			subFinishCntLock.Unlock()
		}()
		actSubCnt++
		select {
		case <-ctx.Done():
			break
		default:
		}
	}
	wg.Wait()
	diff := time.Since(tm)
	log.Println("made", actSubCnt, "subscribers in", diff.Milliseconds(), "ms")
	log.Println(int(diff.Nanoseconds())/(actSubCnt+1), "ns/op")

	time.Sleep(5 * time.Second)
	ticker := time.Tick(1 * time.Second)

	for {
		select {
		case <-ctx.Done():
			break
		case <-ticker:
			for i := 0; i < pubCnt; i++ {
				go func() {
					c2tx, _ := context.WithTimeout(ctx, 1*time.Second)
					bud.Publish(c2tx, "randomString(3)", broker.Message{Body: randomString(20), Expiration: 100 * time.Second})
				}()
			}
		}
	}
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
