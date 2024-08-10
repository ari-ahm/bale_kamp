package main

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sync"
	"time"
)

func main() {
	conn, err := pgxpool.New(context.Background(), "postgres://root:root@localhost:5432/broker")
	if err != nil {
		panic(err)
	}

	mmd := pgx.Batch{}
	wg := sync.WaitGroup{}

	for i := 5; i < 50000; i++ {
		tt := mmd.Queue("INSERT INTO messages (body, subject, expiration) VALUES ($1, $2, $3) RETURNING id", string(i), string(i), time.Now())
		wg.Add(1)
		tt.QueryRow(func(row pgx.Row) error {
			defer wg.Done()
			id := 0
			row.Scan(&id)
			return nil
		})
	}

	tm := time.Now()
	br := conn.SendBatch(context.Background(), &mmd)
	err = br.Close()
	if err != nil {
		panic(err)
	}
	wg.Wait()
	log.Println(time.Since(tm).Milliseconds())
}
