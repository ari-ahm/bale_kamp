package main

import (
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	pong "grpc_learning/proto"
	"log"
	"net"
)

type PongServiceServer struct {
	pong.UnimplementedPongServiceServer
}

func (server *PongServiceServer) Ping(ctx context.Context, in *pong.PingRequest) (w *pong.PongResponse, err error) {
	jobDone := make(chan bool)

	go func() {
		defer func() { jobDone <- true }()

		for i := in.GetReq() + 1; ; i++ {
			prime := true
			for j := 2; j*j <= int(i); j++ {
				if int(i)%j == 0 {
					prime = false
					break
				}
			}
			if prime {
				w = &pong.PongResponse{Resp: i}
				return
			}
		}
	}()

	select {
	case <-jobDone:
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	}
}

func main() {
	port := flag.Int("port", 8080, "server port")
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pong.RegisterPongServiceServer(s, &PongServiceServer{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
