package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums <-chan int, out chan<- int) {
	sum := 0
	for n := range nums {
		sum += n
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	file, err := os.Open(fileName)
	checkError(err)
	ints, err := readInts(file)
	checkError(err)
	results := make(chan int)
	queueSize := len(ints) / num
	for i := 0; i < num; i++ {
		if i == num-1 {
			queueSize = len(ints)
		}
		queue := make(chan int, queueSize)
		for j := 0; j < queueSize; j++ {
			queue <- ints[j]
		}
		close(queue)
		go sumWorker(queue, results)
		ints = ints[queueSize:]
	}

	sum := 0
	for i := 0; i < num; i++ {
		sum += <-results
	}
	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
