// Package cdb reads and writes cdb ("constant database") files.
//
// See the original cdb specification and C implementation by D. J. Bernstein
// at http://cr.yp.to/cdb.html.
package cdbmap

import (
	"bufio"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"runtime"
)

const (
	HeaderSize = uint32(256 * 8)
)

type Cdb struct {
	r      io.ReaderAt
	closer io.Closer
	buf    []byte
	m      map[string][]string
}

// Open opens the named file read-only and returns a new Cdb object.  The file
// should exist and be a cdb-format database file.
func Open(name string) (*Cdb, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	c := New(f)
	c.closer = f
	runtime.SetFinalizer(c, (*Cdb).Close)
	return c, nil
}

// Close closes the cdb for any further reads.
func (c *Cdb) Close() (err error) {
	if c.closer != nil {
		err = c.closer.Close()
		c.closer = nil
		runtime.SetFinalizer(c, nil)
	}
	return err
}

// New creates a new Cdb from the given ReaderAt, which should be a cdb format database.
func New(r io.ReaderAt) *Cdb {
	c := new(Cdb)
	c.r = r
	c.buf = make([]byte, 64)
	return c
}

// NewFromMap creates a new Cdb from the given map[string][]string, which should be
// a map of string keys to arrays of string values.
func NewFromMap(m map[string][]string) *Cdb {
	c := new(Cdb)
	c.r = nil
	c.m = m
	return c
}

// Write takes the map in c.m and writes it to a CDB file on the disk.
func (c *Cdb) Write(f string) (err error) {
	tmp, err := ioutil.TempFile("", f)
	if err != nil { return }
	w, err := os.OpenFile(tmp.Name(), os.O_RDWR | os.O_CREATE, 0644)
	if err != nil { return }

	if _, err = w.Seek(int64(HeaderSize), 0); err != nil {
		return
	}

	wb := bufio.NewWriter(w)
	hash := cdbHash()
	hw := io.MultiWriter(hash, wb)
	htables := make(map[uint32][]slot)

	pos := HeaderSize
	buf := make([]byte, 8)
	for kstring, values := range c.m {
		key := []byte(kstring)
		klen := uint32(len(key))
		for _, dstring := range values {
			data := []byte(dstring)
			dlen := uint32(len(data))
			writeNums(wb, klen, dlen, buf)

			hash.Reset()
			hw.Write(key)
			wb.Write(data)

			h := hash.Sum32()
			tableNum := h % 256
			htables[tableNum] = append(htables[tableNum], slot{h, pos})
			pos += 8 + klen + dlen
		}
	}

	maxSlots := 0
	for _, slots := range htables {
		if len(slots) > maxSlots {
			maxSlots = len(slots)
		}
	}

	slotTable := make([]slot, maxSlots * 2)

	header := make([]byte, HeaderSize)
	for i := uint32(0); i < 256; i++ {
		slots := htables[i]
		if slots == nil {
			putNum(header[i * 8:], pos)
			continue
		}

		nslots := uint32(len(slots) * 2)
		hashSlotTable := slotTable[:nslots]

		for j:= 0; j < len(hashSlotTable); j++ {
			hashSlotTable[j].h = 0
			hashSlotTable[j].pos = 0
		}

		for _, slot := range slots {
			slotPos := (slot.h / 256) % nslots
			if hashSlotTable[slotPos].pos != 0 {
				slotPos++
				if slotPos == uint32(len(hashSlotTable)) {
					slotPos = 0
				}
			}
			hashSlotTable[slotPos] = slot
		}

		if err = writeSlots(wb, hashSlotTable, buf); err != nil {
			return
		}


		putNum(header[i*8:], pos)
		putNum(header[i*8+4:], nslots)
		pos += 8 * nslots
	}

	if err = wb.Flush(); err != nil {
		return
	}

	if _, err = w.Seek(0, 0); err != nil {
		return
	}

	if _, err = w.Write(header); err != nil { return }
	if err = os.Rename(tmp.Name(), f); err != nil { return }

	c.closer = w
	return
}

// Return the map of all the keys/values
func (c *Cdb) Map() (map[string][]string, error) {
	if c.m != nil {
		return c.m, nil
	}

	c.m = make(map[string][]string)

	var klen, dlen uint32
	last, _ := c.readNums(0)
	for pos := HeaderSize; pos < last; pos = pos + 8 + klen + dlen {
		klen, dlen = c.readNums(pos)
		kval := make([]byte, klen)
		dval := make([]byte, dlen)
		if err := c.read(kval, pos + 8); err != nil {
			return nil, err
		}
		if err := c.read(dval, pos + 8 + klen); err != nil {
			return nil, err
		}

		c.m[string(kval)] = append(c.m[string(kval)], string(dval))
	}

	return c.m, nil
}


func (c *Cdb) read(buf []byte, pos uint32) error {
	_, err := c.r.ReadAt(buf, int64(pos))
	return err
}

func (c *Cdb) readNums(pos uint32) (uint32, uint32) {
	if _, err := c.r.ReadAt(c.buf[:8], int64(pos)); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint32(c.buf), binary.LittleEndian.Uint32(c.buf[4:])
}
