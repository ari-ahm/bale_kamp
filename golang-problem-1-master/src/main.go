package main

import (
	"time"
)

func Solution(d time.Duration, message string, ch ...chan string) (numberOfAccesses int) {
	time.Sleep(d * time.Second)
	for _, ch := range ch {
		select {
		case ch <- message:
			numberOfAccesses++
		default:

		}
	}
	return numberOfAccesses
}
