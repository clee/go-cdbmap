# go-cdbmap

go-cdbmap is a pure [Go](http://golang.org/) package to read and write cdb ("constant database") files, forked from [jbarham's go-cdb library](https://github.com/jbarham/go-cdb/).

The cdb file format is a machine-independent format with the following features:

 - *Fast lookups:* A successful lookup in a large database normally takes just two disk accesses. An unsuccessful lookup takes only one.
 - *Low overhead:* A database uses 2048 bytes, plus 24 bytes per record, plus the space for keys and data.
 - *No random limits:* cdb can handle any database up to 4 gigabytes. There are no other restrictions; records don't even have to fit into memory.

See the original cdb specification and C implementation by D. J. Bernstein
at http://cr.yp.to/cdb.html.

## Installation

Assuming you have a working Go environment, installation is simply:

	go get github.com/clee/go-cdbmap

The package documentation can be viewed online at
http://gopkgdoc.appspot.com/pkg/github.com/clee/go-cdbmap
or on the command line by running `go doc github.com/clee/go-cdbmap`.

The usage is extremely simple; here is an example program that shows usage of the entire API.

```go
package main

import (
	"github.com/clee/go-cdbmap"
	"fmt"
	"os"
)

func main() {
	// Read a cdb-formatted file into a map[string][]string
	m, err := cdbmap.FromFile("example.cdb")
	if err != nil {
		panic(err)
	}

	// Or, if you already have the file open...
	r, err := os.Open("example.cdb")
	if err != nil {
		panic(err)
	}
	m, err = cdbmap.Read(r)
	if err != nil {
		panic(err)
	}

	// Now that we have a map, we can loop over the keys...
	for key, values := range m {
		fmt.Printf("key: %s [\n", key)

		// And all of the values, too
		for _, value := range values {
			fmt.Printf("\t%s\n", value)
		}
		fmt.Printf("]\n")
	}

	// Take a map[string][]string and turn it into a cdb file
	cdbmap.ToFile(m, "/tmp/test.cdb")

	// Or, again, if you already have an open writeable file...
	w, err = os.Open("/tmp/test.cdb")
	if err != nil {
		panic(err)
	}
	cdbmap.Write(m, w)
}
```

## Utilities

The go-cdbmap package includes ports of the programs `cdbdump` and `cdbmake` from
the [original implementation](http://cr.yp.to/cdb/cdbmake.html).
