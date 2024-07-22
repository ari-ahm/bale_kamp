# Bale Messenger

### *Description*

You are to implement an **messenger gRPC server**.\
This messenger have 4 rpc:
- `AddUser`
- `SendMessage`
- `FetchMessage`
- `GetUserMessages`

\
The `AddUser` function create new user.\
It has two arguments. The first one is the username of the new user, and the second one is file_id of its profile.\
**username** is unique, and it should have length larger than 3, and it should contain both letters and digits.\
**file_id** of profile must a valid file_id and already uploaded on file server.\
If one of these roles violated, This function returns an error with a message: 
```
invalid username
```
\
In successful situation, it returns an id of the user. Ids of users are
begins from 1 and increase by one in each new creation.
\
\
The `SendMessage` function sends message to the specific user.
As the first parameter, it gets the id or username of the user who wants to send message.
The second parameter is the id or username of the another user that the user wants to send message to.
Finally, the last parameter is the content of the message.\
The content of the message is one of the following modes: **text**, **image**, **file**.
- In case of text message, client send an **string text**.
- In case of image and file message, client send an **file_id** (you must check existing of file)


If not existed user tries to send message, the function returns an error with
the message: 
```
sender user does not exist
```
\
If user tries to send message to not existed user, the function returns an error with the message: 
```
receiver user does not exist
```


The `FetchMessage` function returns the content of specific message with message id.
As the first and only parameter, it gets id of a meesage.
If message with this id does not exist, the function returns an error with the message:
```
message does not exist
```

The `GetUserMessages` function returns content and ids of the messages for specific user. message should aggregated by users and sort based on last message time.
As the first and only parameter, it gets id of a user.
If user with this id does not exist, the function returns an error with the message:
```
user does not exist
```
