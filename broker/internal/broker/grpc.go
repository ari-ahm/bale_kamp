package broker

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"therealbroker/api/proto"
	"therealbroker/pkg/broker"
	"time"
)

var (
	contextTimeout = func() time.Duration {
		ret, err := time.ParseDuration(os.Getenv("CONTEXT_TIMEOUT_CAP"))
		if err != nil {
			panic(err)
		}
		return ret
	}()
)

type brokerServerImpl struct {
	proto.UnimplementedBrokerServer
	module broker.Broker
}

func NewBrokerServer() proto.BrokerServer {
	return &brokerServerImpl{module: NewModule(NewBrokerRepo(), NewBrokerMessageHandler())}
}

func (b *brokerServerImpl) Publish(ctx context.Context, request *proto.PublishRequest) (*proto.PublishResponse, error) {
	pubCtx, _ := context.WithTimeout(ctx, contextTimeout)
	id, err := b.module.Publish(pubCtx, request.GetSubject(), newMessage(request))
	if err != nil {
		return nil, errTranslate(err)
	}

	return &proto.PublishResponse{Id: id}, nil
}

func (b *brokerServerImpl) Subscribe(request *proto.SubscribeRequest, server proto.Broker_SubscribeServer) error {
	ctx := server.Context()
	subCtx, subCtxCancel := context.WithCancel(ctx)
	defer subCtxCancel()
	msgChannel, err := b.module.Subscribe(subCtx, request.GetSubject())
	if err != nil {
		return errTranslate(err)
	}

	for {
		select {
		case msg, ok := <-msgChannel:
			if !ok {
				return nil
			}

			err := server.Send(newMessageResponse(msg))
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return errTranslate(broker.ErrContextCanceled)
		}
	}
}

func (b *brokerServerImpl) Fetch(ctx context.Context, request *proto.FetchRequest) (*proto.MessageResponse, error) {
	fetchCtx, _ := context.WithTimeout(ctx, contextTimeout)
	msg, err := b.module.Fetch(fetchCtx, request.GetSubject(), request.GetId())
	if err != nil {
		return nil, errTranslate(err)
	}

	return newMessageResponse(&msg), nil
}

func newMessage(request *proto.PublishRequest) broker.Message {
	return broker.Message{
		Body:       string(request.GetBody()),
		Expiration: time.Duration(request.GetExpirationSeconds()) * time.Second,
	}
}

func newMessageResponse(msg *broker.Message) *proto.MessageResponse {
	return &proto.MessageResponse{
		Body: []byte(msg.Body),
	}
}

func errTranslate(err error) error {
	switch err {
	case broker.ErrContextCanceled:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case broker.ErrExpiredID:
		return status.Error(codes.PermissionDenied, err.Error())
	case broker.ErrUnavailable:
		return status.Error(codes.Unavailable, err.Error())
	case broker.ErrInvalidID:
		return status.Error(codes.InvalidArgument, err.Error())
	case broker.ErrInternalError, broker.ErrNilPointer:
		return status.Error(codes.Internal, err.Error())
	case broker.ErrExhausted:
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Unknown, err.Error())
	}
}
