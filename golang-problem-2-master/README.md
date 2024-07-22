# Golang-Problem-2


### *Description*

You are to implement the `ReadSendMessageRequest` function.
It gets the filename as the parameter and unmarshals the json that exists 
in that file to the `SendMessage` struct.\
`ChatID` can be integer or string, and it is a required field. If it doesn't exist in json, you are to return an error with
the message : **chat_id is empty**\
`Text` is the text of the message, and it is also required. if it doesn't exist in json, you are
to return an error with the message : **text is empty**\
`ParseMode` has a type string, and it isn't required.\
`ReplyMarkup` has a type ReplyMarkup. It isn't required.
different formats of the `ReplyMarkup` field are listed below:

```json
    {
        "keyboard" : [["string","string"]]
        "resize_keyboard" : "bool"
        "one_time_keyboard" : "bool"
        "selective" : "bool"
    }
```

```json
    {
        "keyboard" : [[{"text" : "string", "request_contact" : "bool", "request_location" : "bool"}]]
        "resize_keyboard" : "bool"
        "one_time_keyboard" : "bool"
        "selective" : "bool"
    }
```

```json
    {
        "inline_keyboard" : [["string", "string"]]
    }
```

```json
    {
        "inline_keyboard" : [[{"text" : "string", "callback_data" : "string", "url" : "string"}]]
    }
```

each of these formats could be string. For example for the first format we may have :

    ""{\"keyboard\" : [[\"string\",\"string\"],[\"string\",\"string\"]]}""
