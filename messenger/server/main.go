package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	messenger "messenger/grpc"
	"messenger/service"
	"messenger/utils"
	"net"
)

func main() {
	port := flag.Int("port", 8080, "The server port")
	flag.Parse()

	conn, err := grpc.NewClient("localhost:8090", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	utils.FileClient = messenger.NewFileServerClient(conn)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	messenger.RegisterMessengerServiceServer(grpcServer, service.NewMessengerService())

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
