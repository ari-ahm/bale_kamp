package broker

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"log"
	"os"
	"sync"
	"therealbroker/pkg/broker"
	"time"
)

var (
	databaseUrl                  = os.Getenv("DATABASE_URL")
	databaseBatchRequestInterval = func() time.Duration {
		ret, err := time.ParseDuration(os.Getenv("DATABASE_BATCH_REQUEST_INTERVAL"))
		if err != nil {
			panic(err)
		}
		return ret
	}()
	databaseRequestLatencyMetric = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "broker_database_request_latency",
		Buckets: []float64{10, 20, 35, 50, 75, 100, 200, 300, 400, 500, 750, 1000, 1250, 1500, 1750, 2000, 2500, 3000, 3500, 4000, 4500, 5000},
	})
)

type brokerRepo struct {
	dbConn       *pgxpool.Pool
	batch        *pgx.Batch
	batchLock    sync.RWMutex
	pubBatchLock sync.Mutex
	ctx          context.Context
	close        context.CancelFunc
}

type BrokerRepo interface {
	io.Closer
	save(ctx context.Context, message *broker.Message, subject string) (id int, err error)
	load(ctx context.Context, id int, subject string) (message *broker.Message, err error)
}

func NewBrokerRepo() BrokerRepo {
	conn, err := pgxpool.New(context.Background(), databaseUrl)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mh := &brokerRepo{
		dbConn: conn,
		batch:  &pgx.Batch{},
		ctx:    ctx,
		close:  cancel,
	}

	commit := func() error {
		mh.batchLock.Lock()
		batch := mh.batch
		mh.batch = &pgx.Batch{}
		mh.batchLock.Unlock()

		br := conn.SendBatch(context.Background(), batch) // TODO fix context(ask)
		err = br.Close()
		if err != nil {
			return err
		}
		return nil
	}

	ticker := time.Tick(databaseBatchRequestInterval)
	go func() {
		for {
			select {
			case <-ticker:
				tm := time.Now()
				err := commit()
				if err != nil {
					log.Println(err)
				}
				databaseRequestLatencyMetric.Observe(float64(time.Since(tm).Milliseconds()))
			case <-ctx.Done():
				return
			}
		}
	}()

	return mh
}

func (r *brokerRepo) save(ctx context.Context, message *broker.Message, subject string) (int, error) {
	var id int64
	err := r.runQuery(ctx, func(row pgx.Row) error {
		return row.Scan(&id)
	}, "INSERT INTO messages (body, subject, expiration) VALUES ($1, $2, $3) RETURNING id", message.Body, subject, time.Now().UTC().Add(message.Expiration))
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
		func(row pgx.Row) error {
			return row.Scan(&message.Body, &actSubject, &expiration)
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
	r.close()
	return nil
}

func (r *brokerRepo) runQuery(ctx context.Context, fn func(pgx.Row) error, query string, args ...any) error {
	finish := make(chan bool, 1)

	var row *pgx.QueuedQuery
	func() {
		r.batchLock.RLock()
		defer r.batchLock.RUnlock() // TODO ask for a better impl for this.(i want to use defer so i had to wrap it in a func(in case it paniced))
		r.pubBatchLock.Lock()
		defer r.pubBatchLock.Unlock()
		row = r.batch.Queue(query, args...)
	}()
	row.QueryRow(func(row pgx.Row) error {
		err := fn(row)
		finish <- true
		return err
	})

	select {
	case <-finish:
	case <-ctx.Done():
		return broker.ErrContextCanceled
	}

	return nil
}

func (r *brokerRepo) addQuery(ctx context.Context, query string, args ...any) error {
	r.batchLock.RLock()
	defer r.batchLock.RUnlock()
	r.pubBatchLock.Lock()
	defer r.pubBatchLock.Unlock()
	r.batch.Queue(query, args...)
	return nil
}
