package model

type Message interface {
	GetId() int64
	SetId(id int64)
	GetMessage() string
	GetSenderId() int64
	GetReceiverId() int64
}

type TextMessage interface {
	Message
}

type FileMessage interface {
	Message
	GetFileId() string
}

type MediaMessage interface {
	Message
	GetMediaId() string
}
