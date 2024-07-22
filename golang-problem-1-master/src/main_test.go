package main

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= 0.01
}

func TestSolution(t *testing.T) {
	a := make(chan string, 1)
	b := make(chan string, 1)
	Solution(2, "ali", a, b)
	assert.Equal(t, "ali", <-a)
	assert.Equal(t, "ali", <-b)
}

func TestSolution2(t *testing.T) {
	a := make(chan string, 1)
	b := make(chan string, 1)
	a = nil
	start := time.Now()
	Solution(2, "ali", a, b)
	delta := time.Since(start)
	assert.Equal(t, true, almostEqual(delta.Seconds(), 2.0))
	assert.Equal(t, "ali", <-b)
}

func TestSolution3(t *testing.T) {
	start := time.Now()
	a := make(chan string, 1)
	b := make(chan string, 1)
	a <- "full"
	go func() { time.Sleep(1 * time.Second); <-a }()
	Solution(2, "ali", a, b)
	assert.Equal(t, "ali", <-b)
	delta := time.Since(start)
	assert.Equal(t, true, almostEqual(delta.Seconds(), 2.0))
}

func TestSolution4(t *testing.T) {
	start := time.Now()
	a := make(chan string, 1)
	b := make(chan string, 1)
	c := make(chan string, 0)
	d := make(chan string, 1)
	x := Solution(3, "ali", a, b, c, d)
	assert.Equal(t, "ali", <-b)
	assert.Equal(t, "ali", <-a)
	assert.Equal(t, "ali", <-d)
	assert.Equal(t, 3, x)
	delta := time.Since(start)
	assert.Equal(t, true, almostEqual(delta.Seconds(), 3.0))
}

func TestSolution5(t *testing.T) {
	a := make(chan string, 10)
	b := make(chan string, 0)
	c := make(chan string, 5)
	x := Solution(1, "salam", a, b, c)
	assert.Equal(t, 1, len(a))
	assert.Equal(t, 1, len(c))
	assert.Equal(t, 0, len(b))
	assert.Equal(t, "salam", <-a)
	assert.Equal(t, 2, x)
}

func TestSolution6(t *testing.T) {
	a := make(chan string, 10)
	b := make(chan string, 1)
	c := make(chan string, 5)
	b <- "full"
	go func() { time.Sleep(2 * time.Second); <-b }()
	x := Solution(1, "salam", a, b, c)
	start := time.Now()
	Solution(2, "hi", a, b, c)
	delta := time.Since(start)
	assert.Equal(t, "hi", <-b)
	assert.Equal(t, "salam", <-a)
	assert.Equal(t, 2, x)
	assert.Equal(t, true, almostEqual(delta.Seconds(), 2.0))
}
