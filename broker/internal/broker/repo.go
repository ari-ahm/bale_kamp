package broker

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"sync"
	"therealbroker/pkg/broker"
	"time"
)

type brokerRepo struct {
	dbConn    *pgxpool.Pool
	batches   [2]*pgx.Batch
	batchSwap sync.WaitGroup
	lock      sync.Mutex // TODO change
}

type BrokerRepo interface {
	io.Closer
	save(ctx context.Context, message *broker.Message, subject string) (id int, err error)
	load(ctx context.Context, id int, subject string) (message *broker.Message, err error)
}

func NewBrokerRepo() BrokerRepo {
	conn, err := pgxpool.New(context.Background(), "postgres://root:root@db:5432/broker?sslmode=disable") // TODO read from env.
	if err != nil {
		log.Fatal(err)
	}

	mh := &brokerRepo{
		dbConn:  conn,
		batches: [2]*pgx.Batch{{}, {}},
	}

	batchLock := sync.Mutex{}
	commit := func() error {
		did := batchLock.TryLock()
		if did {
			defer batchLock.Unlock()

			mh.lock.Lock()
			mh.batches[0] = mh.batches[1]
			mh.batches[1] = &pgx.Batch{}
			mh.lock.Unlock()

			br := conn.SendBatch(context.Background(), mh.batches[0]) // TODO fix context
			err = br.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}

	ticker := time.Tick(50 * time.Millisecond) // TODO env
	go func() {
		for range ticker {
			err := commit()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	return mh
}

func (r *brokerRepo) save(ctx context.Context, message *broker.Message, subject string) (int, error) {
	var id int64
	err := r.runQuery(ctx,
		func(row pgx.Row, finish chan<- bool) error {
			err := row.Scan(&id)
			finish <- true
			return err
		},
		"INSERT INTO messages (body, subject, expiration) VALUES ($1, $2, $3) RETURNING id", message.Body, subject, time.Now().UTC().Add(message.Expiration))
	if err != nil {
		return 0, err
	}
	return int(id), nil
}

func (r *brokerRepo) load(ctx context.Context, id int, subject string) (*broker.Message, error) {
	var message broker.Message
	var actSubject string
	var expiration time.Time
	err := r.runQuery(ctx,
		func(row pgx.Row, finish chan<- bool) error {
			row.Scan(&message.Body, &actSubject, &expiration)
			finish <- true
			return nil
		}, "SELECT body, subject, expiration FROM messages WHERE id = $1 LIMIT 1", id)

	if err != nil {
		return nil, err
	}

	if actSubject != subject {
		return nil, broker.ErrInvalidID
	}

	if expiration.Before(time.Now().UTC()) {
		return nil, broker.ErrExpiredID
	}

	message.Id = id

	return &message, nil
}

func (r *brokerRepo) Close() error {
	r.dbConn.Close()
	return nil
}

func (r *brokerRepo) runQuery(ctx context.Context, fn func(pgx.Row, chan<- bool) error, query string, args ...any) error {
	finish := make(chan bool, 1)

	r.lock.Lock()
	row := r.batches[1].Queue(query, args...)
	row.QueryRow(func(row pgx.Row) error {
		return fn(row, finish)
	})
	r.lock.Unlock()

	select {
	case <-finish:
	case <-ctx.Done():
		return broker.ErrContextCanceled
	}

	return nil
}
