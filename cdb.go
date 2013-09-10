// Package cdbmap reads and writes cdb ("constant database") files.
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
)

const (
	HeaderSize = uint32(256 * 8)
)

// Return the map of all the keys/values
func Read(r io.ReaderAt) (map[string][]string, error) {
	m := make(map[string][]string)
	readNums := makeNumsReader(r)
	read := makeReader(r)

	last, _ := readNums(0)

	var klen, dlen uint32
	for pos := HeaderSize; pos < last; pos = pos + 8 + klen + dlen {
		klen, dlen = readNums(pos)
		kval := make([]byte, klen)
		dval := make([]byte, dlen)
		if err := read(kval, pos + 8); err != nil {
			return nil, err
		}
		if err := read(dval, pos + 8 + klen); err != nil {
			return nil, err
		}

		m[string(kval)] = append(m[string(kval)], string(dval))
	}

	return m, nil
}

// Write takes the map in m and writes it to an io.WriteSeeker
func Write(m map[string][]string, w io.WriteSeeker) (err error) {
	if _, err = w.Seek(int64(HeaderSize), 0); err != nil {
		return
	}

	wb := bufio.NewWriter(w)
	hash := cdbHash()
	hw := io.MultiWriter(hash, wb)
	pos := HeaderSize
	buf := make([]byte, 8)
	htables := make(map[uint32][]slot)

	for kstring, values := range m {
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

	return
}

// FromFile is a convenience function that reads a CDB-formatted
// file from the specified filename, and returns the CDB contents
// in map[string][]string form (or an error if the map can't
// be written for some reason).
func FromFile(filename string) (map[string][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return Read(f)
}

// ToFile is a convenience function that writes a map to the provided
// filename in CDB format.
func ToFile(m map[string][]string, f string) (err error) {
	tmp, err := ioutil.TempFile("", f)
	if err != nil { return }

	w, err := os.OpenFile(tmp.Name(), os.O_RDWR | os.O_CREATE, 0644)
	if err != nil { return }

	r := Write(m, w)
	if err = os.Rename(tmp.Name(), f); err != nil { return }

	return r
}

func makeNumsReader(r io.ReaderAt) (func (uint32) (uint32, uint32)) {
	buf := make([]byte, 64)
	return func(pos uint32) (uint32, uint32) {
		if _, err := r.ReadAt(buf[:8], int64(pos)); err != nil {
			panic(err)
		}
		return binary.LittleEndian.Uint32(buf), binary.LittleEndian.Uint32(buf[4:])
	}
}

func makeReader(r io.ReaderAt) (func ([]byte, uint32) error) {
	return func(buf []byte, pos uint32) error {
		_, err := r.ReadAt(buf, int64(pos))
		return err
	}
}
