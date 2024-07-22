package service

import (
	"errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"messenger/model"
	"messenger/repository"
	"messenger/utils"
)

type abstractMessage struct {
	messageId      int64
	message        string
	senderUserId   int64
	receiverUserId int64
}

func (m *abstractMessage) GetId() int64 {
	return m.messageId
}
func (m *abstractMessage) SetId(id int64) {
	m.messageId = id
}
func (m *abstractMessage) GetMessage() string {
	return m.message
}
func (m *abstractMessage) GetSenderId() int64 {
	return m.senderUserId
}
func (m *abstractMessage) GetReceiverId() int64 {
	return m.receiverUserId
}

type textMessage struct {
	abstractMessage
}

type fileMessage struct {
	abstractMessage
	fileId string
}

func (m *fileMessage) GetFileId() string {
	return m.fileId
}

type mediaMessage struct {
	abstractMessage
	mediaId string
}

func (m *mediaMessage) GetMediaId() string {
	return m.mediaId
}

func newAbstractMessage(message string, sender, receiver int64, userRepo repository.UserRepository) (abstractMessage, error) {
	if !userRepo.Contains(sender) || !userRepo.Contains(receiver) {
		return abstractMessage{}, status.Error(codes.InvalidArgument, "sending or receiving user does not exist")
	}
	return abstractMessage{message: message, senderUserId: sender, receiverUserId: receiver}, nil
}

func NewTextMessage(message string, sender, receiver int64, userRepo repository.UserRepository) (model.TextMessage, error) {
	if message == "" {
		return nil, errors.New("message cannot be empty")
	}

	base, err := newAbstractMessage(message, sender, receiver, userRepo)
	if err != nil {
		return nil, err
	}

	return &textMessage{base}, nil
}

func NewFileMessage(message, fileId string, sender, receiver int64, userRepo repository.UserRepository) (model.FileMessage, error) {
	ok, err := utils.FileExists(fileId)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "file not found")
	}

	base, err := newAbstractMessage(message, sender, receiver, userRepo)
	if err != nil {
		return nil, err
	}

	return &fileMessage{abstractMessage: base, fileId: fileId}, nil
}

func NewMediaMessage(message, fileId string, sender, receiver int64, userRepo repository.UserRepository) (model.MediaMessage, error) {
	ok, err := utils.FileExists(fileId)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "media file not found")
	}

	base, err := newAbstractMessage(message, sender, receiver, userRepo)
	if err != nil {
		return nil, err
	}

	return &mediaMessage{abstractMessage: base, mediaId: fileId}, nil
}
