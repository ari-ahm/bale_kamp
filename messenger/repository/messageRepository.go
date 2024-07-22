package repository

import (
	"errors"
	"messenger/model"
	"reflect"
)

type MessageRepository interface {
	Save(message model.Message) error
	GetById(messageId int64) (model.Message, error)
	GetUserMessages(userId int64) (map[int64][]model.Message, error)
}

type messageRepositoryImpl struct {
	messages         map[int64]model.Message
	incomingMessages map[int64]map[int64][]model.Message
	messageIdCnt     int64
}

func NewMessageRepository() MessageRepository {
	return &messageRepositoryImpl{
		make(map[int64]model.Message),
		make(map[int64]map[int64][]model.Message),
		1,
	}
}

func (m *messageRepositoryImpl) Save(message model.Message) error {
	if reflect.ValueOf(message.GetId()).IsZero() {
		message.SetId(m.messageIdCnt)
		m.messageIdCnt++
	}
	m.messages[message.GetId()] = message

	if _, ok := m.incomingMessages[message.GetReceiverId()]; !ok {
		m.incomingMessages[message.GetReceiverId()] = make(map[int64][]model.Message)
	}
	if _, ok := m.incomingMessages[message.GetReceiverId()][message.GetSenderId()]; !ok {
		m.incomingMessages[message.GetReceiverId()][message.GetSenderId()] = make([]model.Message, 0)
	}

	m.incomingMessages[message.GetReceiverId()][message.GetSenderId()] = append(m.incomingMessages[message.GetReceiverId()][message.GetSenderId()], message)

	return nil
}

func (m *messageRepositoryImpl) GetById(messageId int64) (model.Message, error) {
	message, ok := m.messages[messageId]
	if !ok {
		return message, errors.New("user not found")
	}

	return message, nil
}

func (m *messageRepositoryImpl) GetUserMessages(userId int64) (map[int64][]model.Message, error) {
	return m.incomingMessages[userId], nil
}
