package broker

import (
	"context"
	"github.com/gocql/gocql"
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
	databaseKeyspace             = os.Getenv("DATABASE_KEYSPACE")
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
	dbConn       *gocql.Session
	batch        *gocql.Batch
	batchLock    sync.RWMutex
	pubBatchLock sync.Mutex
	ctx          context.Context
	close        context.CancelFunc
}

type BrokerRepo interface {
	io.Closer
	save(ctx context.Context, message *broker.Message, subject string) (id string, err error)
	load(ctx context.Context, id string, subject string) (message *broker.Message, err error)
}

func NewBrokerRepo() BrokerRepo {
	cluster := gocql.NewCluster(databaseUrl)
	cluster.Keyspace = databaseKeyspace
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mh := &brokerRepo{
		dbConn: session,
		//batch:  session.NewBatch(gocql.LoggedBatch),
		ctx:   ctx,
		close: cancel,
	}

	//commit := func() error {
	//	mh.batchLock.Lock()
	//	batch := mh.batch
	//	mh.batch = session.NewBatch(gocql.UnloggedBatch)
	//	mh.batchLock.Unlock()
	//
	//	err := session.ExecuteBatch(batch)
	//	if err != nil {
	//		return err
	//	}
	//	return nil
	//}
	//
	//ticker := time.Tick(databaseBatchRequestInterval)
	//go func() {
	//	for {
	//		select {
	//		case <-ticker:
	//			tm := time.Now()
	//			err := commit()
	//			if err != nil {
	//				log.Println(err)
	//			}
	//			databaseRequestLatencyMetric.Observe(float64(time.Since(tm).Milliseconds()))
	//		case <-ctx.Done():
	//			return
	//		}
	//	}
	//}()

	return mh
}

func (r *brokerRepo) save(ctx context.Context, message *broker.Message, subject string) (string, error) {
	id := gocql.MustRandomUUID()
	message.Id = id.String()
	ttl := message.Expiration / time.Second
	if ttl != 0 {
		err := r.dbConn.Query("INSERT INTO messages (id, body, subject) VALUES (?, ?, ?) USING TTL ?", id, message.Body, subject, ttl).WithContext(ctx).Exec()
		if err != nil {
			return "", err
		}
	}
	return message.Id, nil
}

func (r *brokerRepo) load(ctx context.Context, id string, subject string) (*broker.Message, error) {
	var message broker.Message

	message.Id = id
	err := r.dbConn.Query("SELECT body FROM messages WHERE id=? AND subject=? LIMIT 1", id, subject).WithContext(ctx).Scan(&message.Body)
	if err == gocql.ErrNotFound {
		return nil, broker.ErrInvalidID
	}

	if err != nil {
		return nil, err
	}

	return &message, nil
}

func (r *brokerRepo) Close() error {
	r.dbConn.Close()
	r.close()
	return nil
}

//func (r *brokerRepo) runQuery(ctx context.Context, fn func(pgx.Row) error, query string, args ...any) error {
//	finish := make(chan bool, 1)
//
//	var row *pgx.QueuedQuery
//	func() {
//		r.batchLock.RLock()
//		defer r.batchLock.RUnlock() // TODO ask for a better impl for this.(i want to use defer so i had to wrap it in a func(in case it paniced))
//		r.pubBatchLock.Lock()
//		defer r.pubBatchLock.Unlock()
//		row = r.batch.Queue(query, args...)
//	}()
//	row.QueryRow(func(row pgx.Row) error {
//		err := fn(row)
//		finish <- true
//		return err
//	})
//
//	select {
//	case <-finish:
//	case <-ctx.Done():
//		return broker.ErrContextCanceled
//	}
//
//	return nil
//}
//
//func (r *brokerRepo) addQuery(ctx context.Context, query string, args ...any) error {
//	r.batchLock.RLock()
//	defer r.batchLock.RUnlock()
//	r.pubBatchLock.Lock()
//	defer r.pubBatchLock.Unlock()
//	r.batch.Queue(query, args...)
//	return nil
//}
