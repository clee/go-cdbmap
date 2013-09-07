package main

import (
	"bufio"
	"github.com/clee/go-cdbmap"
	"os"
)

func main() {
	bin, bout := bufio.NewReader(os.Stdin), bufio.NewWriter(os.Stdout)
	err := cdbmap.Dump(bout, bin)
	bout.Flush()
	if err != nil {
		os.Exit(111)
	}
}
