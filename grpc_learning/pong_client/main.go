package main

import (
	"context"
	"flag"
	"google.golang.org/grpc"
	pong "grpc_learning/proto"
	"log"
	"time"
)

func main() {
	num := flag.Int("n", 10, "ping :)")
	addr := flag.String("Address", "localhost:8080", "Server addr")
	flag.Parse()
	conn, err := grpc.NewClient(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()
	c := pong.NewPongServiceClient(conn)
	for i := *num; ; {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		log.Printf("ping: %v", i)
		r, err := c.Ping(ctx, &pong.PingRequest{Req: int32(i)})
		if err != nil {
			log.Fatalf("could not ping: %v", err)
		}
		cancel()
		log.Printf("pong: %v", r.Resp)
		i = int(r.Resp)
	}
}
