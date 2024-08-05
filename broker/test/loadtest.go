package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
	"therealbroker/api/proto"
	"therealbroker/pkg/broker"
	"time"
)

type brokerSpy struct {
	client proto.BrokerClient
}

func (b brokerSpy) Close() error {
	return nil
}

func (b brokerSpy) Publish(ctx context.Context, subject string, msg broker.Message) (int, error) {
	res, err := b.client.Publish(context.WithoutCancel(ctx), &proto.PublishRequest{
		Subject:           subject,
		Body:              []byte(msg.Body),
		ExpirationSeconds: int32((msg.Expiration + time.Second - 1) / time.Second),
	})

	return int(res.GetId()), err
}

func (b brokerSpy) Subscribe(ctx context.Context, subject string) (<-chan broker.Message, error) {
	res, err := b.client.Subscribe(context.WithoutCancel(ctx), &proto.SubscribeRequest{
		Subject: subject,
	})

	ch := make(chan broker.Message)

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

			ch <- broker.Message{
				Body: string(in.GetBody()),
			}
		}
	}()

	return ch, err
}

func (b brokerSpy) Fetch(ctx context.Context, subject string, id int) (broker.Message, error) {
	res, err := b.client.Fetch(context.WithoutCancel(ctx), &proto.FetchRequest{
		Subject: subject,
		Id:      int32(id),
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

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(*deadline)*time.Second)
	actSubCnt := 0
	subFinishCnt := 0
	subFinishCntLock := sync.Mutex{}
	wg := sync.WaitGroup{}
	tm := time.Now()
	for i := 0; i < subCnt; i++ {
		wg.Add(1)
		go func() {
			ch, _ := bud.Subscribe(ctx, "ali")
			wg.Done()
			cnt := 0
			for i := range ch {
				if i.Body == "slm" {
					cnt++
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
	diff := time.Since(tm)
	log.Println("made", actSubCnt, "subscribers in", diff.Milliseconds(), "ms")
	log.Println(int(diff.Nanoseconds())/(actSubCnt+1), "ns/op")

	actPubCnt := 0
	tm = time.Now()
	for i := 0; i < pubCnt; i++ {
		wg.Add(1)
		//go func() {
		wg.Done()
		bud.Publish(ctx, "ali", broker.Message{Body: "slm", Expiration: 1 * time.Second})
		//}()
		actPubCnt++
		select {
		case <-ctx.Done():
			break
		default:
		}
	}
	wg.Wait()
	diff = time.Since(tm)
	log.Println("made", actPubCnt, "publishes in", diff.Milliseconds(), "ms")
	log.Println(int(diff.Nanoseconds())/(actPubCnt+1), "ns/op")
	time.Sleep(5 * time.Second)
	log.Println(subFinishCnt, "subscribers got all the messages")
}
