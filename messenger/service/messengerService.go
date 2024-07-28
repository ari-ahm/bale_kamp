package service

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	messenger "messenger/grpc"
	"messenger/model"
	"messenger/repository"
	"messenger/utils"
)

var (
	userRepo    = repository.NewUserRepository()
	messageRepo = repository.NewMessageRepository()
)

type messengerService struct {
	messenger.UnimplementedMessengerServiceServer
}

func NewMessengerService() messenger.MessengerServiceServer {
	return &messengerService{}
}

func (m *messengerService) AddUser(ctx context.Context, req *messenger.AddUserRequest) (*messenger.AddUserResponse, error) {
	if _, err := userRepo.GetByUsername(req.GetUsername()); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "user already exists")
	}

	ok, err := utils.FileExists(ctx, req.GetProfilePic())
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Profile pic not found")
	}

	user := model.User{Username: req.GetUsername(), ProfilePic: req.GetProfilePic()}
	err = userRepo.Save(&user)
	if err != nil {
		return nil, err
	}

	return &messenger.AddUserResponse{UserId: user.UserId}, nil
}

func (m *messengerService) SendMessage(ctx context.Context, request *messenger.SendMessageRequest) (*messenger.SendMessageResponse, error) {
	sendingUserId, err := getUserId(request.GetSendingUser())
	if err != nil {
		return nil, err
	}
	receivingUserId, err := getUserId(request.GetReceivingUser())
	if err != nil {
		return nil, err
	}

	message, err := getMessage(ctx, request.GetMessage(), sendingUserId, receivingUserId, userRepo)
	if err != nil {
		return nil, err
	}

	err = messageRepo.Save(message)
	if err != nil {
		return nil, err
	}

	return &messenger.SendMessageResponse{MessageId: message.GetId()}, nil
}

func (m *messengerService) FetchMessage(ctx context.Context, request *messenger.FetchMessageRequest) (*messenger.MessageResponse, error) {
	message, err := messageRepo.GetById(request.GetMessageId())
	if err != nil {
		return nil, err
	}

	return getMessageResponse(message)
}

func (m *messengerService) GetUserMessages(ctx context.Context, request *messenger.GetUserMessagesRequest) (*messenger.GetUserMessagesResponse, error) {
	user, err := getUserId(request.GetUser())
	if err != nil {
		return nil, err
	}

	messages, err := messageRepo.GetUserMessages(user)
	if err != nil {
		return nil, err
	}

	ret := &messenger.GetUserMessagesResponse{
		Messages: make(map[int64]*messenger.MultipleMessageResponse),
	}
	for sender, messages := range messages {
		chatList := &messenger.MultipleMessageResponse{
			Messages: make([]*messenger.MessageResponse, 0),
		}
		ret.Messages[sender] = chatList
		for _, message := range messages {
			messageResponse, err := getMessageResponse(message)
			if err != nil {
				return nil, err
			}

			chatList.Messages = append(chatList.Messages, messageResponse)
		}
	}

	return ret, nil
}

func getUserId(request *messenger.GetUserRequest) (int64, error) {
	switch t := request.GetUser().(type) {
	case *messenger.GetUserRequest_UserId:
		return t.UserId, nil
	case *messenger.GetUserRequest_Username:
		user, err := userRepo.GetByUsername(t.Username)
		if err != nil {
			return 0, status.Error(codes.Internal, err.Error())
		}
		return user.UserId, nil
	default:
		return 0, status.Error(codes.InvalidArgument, "invalid user type in GetUserRequest")
	}
}

func getMessage(ctx context.Context, request *messenger.Message, sender, receiver int64, userRepo repository.UserRepository) (model.Message, error) {
	switch t := request.GetMessage().(type) {
	case *messenger.Message_TextMessage:
		return NewTextMessage(t.TextMessage.GetText(), sender, receiver, userRepo)
	case *messenger.Message_FileMessage:
		return NewFileMessage(ctx, t.FileMessage.GetText(), t.FileMessage.GetFileId(), sender, receiver, userRepo)
	case *messenger.Message_ImageMessage:
		return NewMediaMessage(ctx, t.ImageMessage.GetText(), t.ImageMessage.GetFileId(), sender, receiver, userRepo)
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid type in GetMessage")
	}
}

func getMessageResponse(message model.Message) (*messenger.MessageResponse, error) {
	switch t := message.(type) {
	case model.TextMessage:
		return &messenger.MessageResponse{
			MessageId: t.GetId(),
			Message: &messenger.Message{
				Message: &messenger.Message_TextMessage{
					TextMessage: &messenger.TextMessage{
						Text: t.GetMessage(),
					},
				},
			},
		}, nil
	case model.FileMessage:
		return &messenger.MessageResponse{
			MessageId: message.GetId(),
			Message: &messenger.Message{
				Message: &messenger.Message_FileMessage{
					FileMessage: &messenger.FileMessage{
						Text:   t.GetMessage(),
						FileId: t.GetFileId(),
					},
				},
			},
		}, nil
	case model.MediaMessage:
		return &messenger.MessageResponse{
			MessageId: message.GetId(),
			Message: &messenger.Message{
				Message: &messenger.Message_ImageMessage{
					ImageMessage: &messenger.ImageMessage{
						Text:   t.GetMessage(),
						FileId: t.GetMediaId(),
					},
				},
			},
		}, nil
	default:
		return nil, status.Error(codes.Unimplemented, "type response not implemented")
	}
}
