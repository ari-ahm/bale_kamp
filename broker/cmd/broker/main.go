package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
	"therealbroker/api/proto"
	"therealbroker/internal/broker"
)

// Main requirements:
// 1. All tests should be passed
// 2. Your logs should be accessible in Graylog
// 3. Basic prometheus metrics ( latency, throughput, etc. ) should be implemented
// 	  for every base functionality ( publish, subscribe etc. )

func main() {
	lis, err := net.Listen("tcp", ":50051") // TODO read from env
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(broker.MetricsUnaryInterceptor),
		grpc.StreamInterceptor(broker.MetricsStreamInterceptor),
	)

	proto.RegisterBrokerServer(grpcServer, broker.NewBrokerServer())

	// TODO ask
	//broker.InitMetrics()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
