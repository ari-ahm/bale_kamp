package repository

import (
	"errors"
	"messenger/model"
	"reflect"
)

type UserRepository interface {
	Save(user *model.User) error
	GetByUsername(username string) (model.User, error)
	GetById(userId int64) (model.User, error)
	Contains(userId int64) bool
}

type userRepositoryImpl struct {
	users     map[int64]model.User
	userIds   map[string]int64
	userIdCnt int64
}

func NewUserRepository() UserRepository {
	return &userRepositoryImpl{
		make(map[int64]model.User),
		make(map[string]int64),
		1,
	}
}

func (m *userRepositoryImpl) Save(user *model.User) error {
	if reflect.ValueOf(user.UserId).IsZero() {
		user.UserId = m.userIdCnt
		m.userIdCnt++
	}
	m.users[user.UserId] = *user
	m.userIds[user.Username] = user.UserId
	return nil
}

func (m *userRepositoryImpl) GetByUsername(username string) (model.User, error) {
	userId, ok := m.userIds[username]
	if !ok {
		return model.User{}, errors.New("user not found")
	}

	return m.users[userId], nil
}

func (m *userRepositoryImpl) GetById(userId int64) (model.User, error) {
	user, ok := m.users[userId]
	if !ok {
		return model.User{}, errors.New("user not found")
	}

	return user, nil
}

func (m *userRepositoryImpl) Contains(userId int64) bool {
	_, ok := m.users[userId]
	return ok
}
