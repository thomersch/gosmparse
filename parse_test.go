package gosmparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aybabtme/iocontrol"
	"github.com/dustin/go-humanize"
	"github.com/facebookgo/ensure"
)

type mockOSMReader struct {
	Nodes     *uint64
	Ways      *uint64
	Relations *uint64
}

func newMockOSMReader() *mockOSMReader {
	return &mockOSMReader{Nodes: new(uint64), Ways: new(uint64), Relations: new(uint64)}
}

func (r mockOSMReader) ReadNode(n Node) {
	atomic.AddUint64(r.Nodes, 1)
}

func (r mockOSMReader) ReadWay(w Way) {
	atomic.AddUint64(r.Ways, 1)
}

func (r mockOSMReader) ReadRelation(rel Relation) {
	atomic.AddUint64(r.Relations, 1)
}

func TestParse(t *testing.T) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		t.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}
	f, err := os.Open(testfile)
	ensure.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	rdr := newMockOSMReader()
	err = Decode(mr, rdr)
	fmt.Printf("Speed: %v/s, total read: %v\n", humanize.Bytes(mr.BytesPerSec()), humanize.Bytes(uint64(mr.Total())))
	fmt.Printf("Read %v nodes, %v ways, %v relations\n", atomic.LoadUint64(rdr.Nodes), atomic.LoadUint64(rdr.Ways), atomic.LoadUint64(rdr.Relations))
	ensure.Nil(t, err)
}

func BenchmarkReadBlock(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	ensure.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(b, err)

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		decoder := newDecoder()
		decoder.Block(reader)
	}
}

func BenchmarkReadMinimalFile(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	ensure.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(b, err)

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		or := newMockOSMReader()
		err := Decode(reader, or)
		ensure.Nil(b, err)
	}
}
