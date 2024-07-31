package main

import (
	"fmt"
	"sync/atomic"
)

func main() {
	var v atomic.Int64
	fmt.Println(v.Load())
	v.Add(1)
	fmt.Println(v.Load())
}
