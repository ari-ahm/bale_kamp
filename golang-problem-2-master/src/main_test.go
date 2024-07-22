package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSendMessageSample1(t *testing.T) {
	msg, err := ReadSendMessageRequest("input_sample2.json")
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	assert.EqualValues(t, "456", msg.ChatID)
	assert.IsType(t, ReplyMarkup{}, msg.ReplyMarkup)
	assert.IsType(t, msg.ReplyMarkup.(ReplyMarkup).InlineKeyboard, [][]InlineKeyboardButton{})
	assert.Equal(t, 2, len(msg.ReplyMarkup.(ReplyMarkup).InlineKeyboard))
	assert.Equal(t, 3, len(msg.ReplyMarkup.(ReplyMarkup).InlineKeyboard[0]))
	assert.Equal(t, "bale", msg.ReplyMarkup.(ReplyMarkup).InlineKeyboard[0][1].Text)
	assert.Equal(t, "HTML", msg.ParseMode)
}

func TestSendMessageSample2(t *testing.T) {
	msg, err := ReadSendMessageRequest("input_sample1.json")
	assert.NotNil(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, "chat_id is empty", err.Error())
}
