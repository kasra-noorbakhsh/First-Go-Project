// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

// A basic test for example
func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestYourFirstGoTest(t *testing.T) {
	fmt.Println("Running TestYourFirstGoTest.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		defer close(input)
		for i := -3; i <= 3; i++ {
			input <- i
		}
	}()
	expectedResults := []int{9, 4, 1, 0, 1, 4, 9}
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	for _, expected := range expectedResults {
		select {
		case <-timeoutChan:
			t.Error("Test timed out.")
		case result := <-squares:
			if result != expected {
				t.Error("Error, got result", result, ", expected", expected)
			}
		}
	}
	sq.Close()
}
