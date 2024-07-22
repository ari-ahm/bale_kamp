package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
)

type KeyboardButton struct {
	Text            string `json:"text"`
	RequestContact  bool   `json:"request_contact"`
	RequestLocation bool   `json:"request_location"`
}

type InlineKeyboardButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
	Url          string `json:"url"`
}

type ReplyMarkup struct {
	InlineKeyboard [][]InlineKeyboardButton `json:"inline_keyboard"`
	Keyboard       [][]KeyboardButton       `json:"keyboard"`
	ResizeKeyboard bool                     `json:"resize_keyboard"`
	OnTimeKeyboard bool                     `json:"one_time_keyboard"`
	Selective      bool                     `json:"selective"`
}

type SendMessage struct {
	ChatID      interface{} `json:"chat_id"`
	Text        string      `json:"text"`
	ParseMode   string      `json:"parse_mode"`
	ReplyMarkup interface{} `json:"reply_markup"`
}

func readFromFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

func (k *KeyboardButton) new(data interface{}) error {
	switch v := data.(type) {
	case string:
		k.Text = v
	case map[string]interface{}:
		if val, ok := v["text"].(string); ok {
			k.Text = val
		} else {
			return errors.New("text must be present when using keyboard and be string")
		}
		k.RequestContact, _ = v["request_contact"].(bool)
		k.RequestLocation, _ = v["request_location"].(bool)
	default:
		return errors.New("invalid KeyboardButton data type")
	}

	return nil
}

func (k *InlineKeyboardButton) new(data interface{}) error {
	switch v := data.(type) {
	case string:
		k.Text = v
	case map[string]interface{}:
		if val, ok := v["text"].(string); ok {
			k.Text = val
		} else {
			return errors.New("text must be present when using inline keyboard and be string")
		}
		k.CallbackData, _ = v["callback_data"].(string)
		k.Url, _ = v["url"].(string)
	default:
		return errors.New("invalid KeyboardButton data type")
	}

	return nil
}

func (r *ReplyMarkup) new(mp interface{}) error {
	if _, ok := mp.(map[string]interface{}); !ok {
		return nil
	}

	data := mp.(map[string]interface{})
	if keyboard, ok := data["keyboard"].([]interface{}); ok {
		if val, ok := data["resize_keyboard"].(bool); ok {
			r.ResizeKeyboard = val
		} else {
			return errors.New("resize_keyboard must be present when using keyboard and be bool")
		}
		if val, ok := data["one_time_keyboard"].(bool); ok {
			r.OnTimeKeyboard = val
		} else {
			return errors.New("one_time_keyboard must be present when using keyboard and be bool")
		}
		if val, ok := data["selective"].(bool); ok {
			r.Selective = val
		} else {
			return errors.New("selective must be present when using keyboard and be bool")
		}

		r.Keyboard = make([][]KeyboardButton, len(keyboard))
		for i, rawRow := range keyboard {
			if row, ok := rawRow.([]interface{}); !ok {
				return errors.New("keyboard must be a 2D array")
			} else {
				r.Keyboard[i] = make([]KeyboardButton, len(row))
				for j, cell := range row {
					if err := r.Keyboard[i][j].new(cell); err != nil {
						return err
					}
				}
			}
		}
	} else if keyboard, ok := data["inline_keyboard"].([]interface{}); ok {
		r.InlineKeyboard = make([][]InlineKeyboardButton, len(keyboard))
		for i, rawRow := range keyboard {
			if row, ok := rawRow.([]interface{}); !ok {
				return errors.New("inline_keyboard must be a 2D array")
			} else {
				r.InlineKeyboard[i] = make([]InlineKeyboardButton, len(row))
				for j, cell := range row {
					if err := r.InlineKeyboard[i][j].new(cell); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (m *SendMessage) init() error {
	switch m.ChatID.(type) {
	case string, int:
	case nil:
		return errors.New("chat_id is empty")
	default:
		return errors.New("chat_id must be a string or integer")
	}

	if m.Text == "" {
		return errors.New("text is empty")
	}

	var replyMarkup ReplyMarkup
	if err := replyMarkup.new(m.ReplyMarkup); err != nil {
		return err
	}
	m.ReplyMarkup = replyMarkup

	return nil
}

func ReadSendMessageRequest(fileName string) (*SendMessage, error) {
	jsonData, err := readFromFile(fileName)
	if err != nil {
		return nil, err
	}

	var ret SendMessage
	if err := json.Unmarshal(jsonData, &ret); err != nil {
		return nil, err
	}

	if err := ret.init(); err != nil {
		return nil, err
	}

	return &ret, nil
}
