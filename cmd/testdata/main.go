// testdata is used to generate random data for testing JotFS
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
)

var (
	seed = flag.Int64("seed", 1278, "random number generator seed")
	size = flag.Uint("size", 0, "size of data")
)

func main() {
	flag.Parse()
	rand.Seed(*seed)

	size := *size
	buf := make([]byte, 4096)
	for n := uint(0); n < size; {
		if rem := size - n; rem < 4096 {
			buf = buf[:rem]
		}
		rand.Read(buf)
		n += uint(len(buf))
		if _, err := os.Stdout.Write(buf); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			os.Exit(1)
		}
	}
}
