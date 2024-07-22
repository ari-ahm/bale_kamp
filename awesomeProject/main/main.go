package main

import (
	"fmt"
	"time"
)

func main() {
	for i := 0; i < 10; i++ {
		j := i
		go func() {
			time.Sleep(time.Second)
			fmt.Println(i, j)
		}()
	}

	time.Sleep(2 * time.Second)
}
