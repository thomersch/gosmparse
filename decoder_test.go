package gosmparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thomersch/gosmparse/OSMPBF"

	"github.com/aybabtme/iocontrol"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
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

type cachedReader struct {
	Mtx   sync.Mutex
	Nodes []Node
	Ways  []Way
	Rels  []Relation
}

func (r *cachedReader) ReadNode(n Node) {
	r.Mtx.Lock()
	defer r.Mtx.Unlock()
	r.Nodes = append(r.Nodes, n)
}

func (r *cachedReader) ReadWay(w Way) {
	r.Mtx.Lock()
	defer r.Mtx.Unlock()
	r.Ways = append(r.Ways, w)
}

func (r *cachedReader) ReadRelation(rel Relation) {
	r.Mtx.Lock()
	defer r.Mtx.Unlock()
	r.Rels = append(r.Rels, rel)
}

func TestParse(t *testing.T) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		t.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}
	f, err := os.Open(testfile)
	assert.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	rdr := newMockOSMReader()
	d := NewDecoder(mr)
	err = d.Parse(rdr)
	fmt.Printf("Speed: %v/s, total read: %v\n", humanize.Bytes(mr.BytesPerSec()), humanize.Bytes(uint64(mr.Total())))
	fmt.Printf("Read %v nodes, %v ways, %v relations\n", atomic.LoadUint64(rdr.Nodes), atomic.LoadUint64(rdr.Ways), atomic.LoadUint64(rdr.Relations))
	assert.Nil(t, err)
}

func TestMinimalParse(t *testing.T) {
	testFile, err := os.Open("testdata/relation.pbf")
	assert.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(t, err)

	reader := bytes.NewReader(buf)
	or := &cachedReader{}
	dec := NewDecoder(reader)
	err = dec.Parse(or)
	assert.Nil(t, err)

	assert.Equal(t, or.Rels[0].Members[0].ID, int64(15))
	assert.Equal(t, or.Rels[0].Members[0].ID, int64(15))
	assert.Equal(t, or.Rels[0].Members[1].ID, int64(16))
	assert.Equal(t, or.Rels[0].Members[2].ID, int64(17))
	assert.Equal(t, or.Rels[0].Members[3].ID, int64(20))
	assert.Equal(t, or.Rels[0].Members[4].ID, int64(100))
	assert.Equal(t, or.Rels[0].Members[5].ID, int64(101))
	assert.Equal(t, or.Rels[0].Members[6].ID, int64(102))
	assert.Equal(t, or.Rels[0].Members[7].ID, int64(98))
	assert.Equal(t, or.Rels[1].Members[0].ID, int64(1))
}

func TestParseHistory(t *testing.T) {
	testFile, err := os.Open("testdata/history.osh.pbf")
	assert.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(t, err)

	reader := bytes.NewReader(buf)
	or := &cachedReader{}
	dec := NewDecoderWithInfo(reader)
	err = dec.Parse(or)
	assert.Nil(t, err)

	assert.Equal(t, or.Nodes[0].Lat, float64(0.001))
	assert.Equal(t, or.Nodes[0].Info, &Info{
		Visible:   true,
		Timestamp: time.Unix(1446404400, 0),
		UID:       1,
		Changeset: 1,
		Version:   1,
		User:      "Dummy User",
	})
	assert.Equal(t, or.Nodes[2].Lat, 0.003)
	assert.Equal(t, or.Nodes[2].Info.Timestamp, time.Unix(1554145200, 0))
	assert.Equal(t, or.Nodes[3].Info.Visible, false)

	assert.Equal(t, or.Ways[0].Info.Changeset, int64(1))
	assert.Equal(t, or.Ways[1].Info.Changeset, int64(2))
	assert.Equal(t, or.Ways[1].Info.User, "Another User")

	assert.Equal(t, or.Rels[0].Members[0].ID, int64(1))
	assert.Equal(t, or.Rels[0].Members[0].Type, NodeType)
	assert.Equal(t, or.Rels[0].Members[1].ID, int64(1))
	assert.Equal(t, or.Rels[0].Members[1].Type, WayType)
	assert.Equal(t, or.Rels[0].Info.Visible, true)
	assert.Equal(t, or.Rels[0].Info.User, "Dummy User")
	assert.Equal(t, or.Rels[1].Info.Visible, false)
	assert.Equal(t, or.Rels[1].Info.UID, 2)
}

func TestBlobDataUncompressed(t *testing.T) {
	originalPrimBlock := &OSMPBF.PrimitiveBlock{
		Stringtable: &OSMPBF.StringTable{},
	}
	primitiveBlockBytes, err := proto.Marshal(originalPrimBlock)
	assert.Nil(t, err)
	blob := &OSMPBF.Blob{
		Raw: primitiveBlockBytes,
	}

	d := NewDecoder(nil)
	primBlock, err := d.blobData(blob)
	assert.Nil(t, err)
	assert.Equal(t, primBlock.Stringtable, originalPrimBlock.Stringtable)
}

func TestCorruptFiles(t *testing.T) {
	fls := []struct {
		Name string
		Buf  []byte
		Err  bool
	}{
		{"empty", []byte{}, true},
		{"bad header", []byte{0, 0, 0, 13, 10, 9, 78, 83, 77, 72, 101, 97, 100, 101, 114, 24, 115, 16, 105, 26, 111, 120, 156, 83, 226, 243, 47, 206, 13, 78, 206, 72, 205, 77, 212, 13, 51, 208, 51, 83, 226, 114, 73, 205, 43, 78, 245, 203, 79, 73, 45, 214, 18, 12, 206, 47, 42, 209, 11, 169, 44, 72, 141, 47, 201, 72, 205, 139, 247, 116, 105, 98, 20, 200, 47, 206, 77, 206, 207, 43, 75, 45, 42, 81, 48, 208, 179, 208, 51, 233, 98, 84, 201, 40, 41, 41, 176, 210, 215, 47, 47, 47, 215, 203, 47, 0, 106, 47, 41, 74, 77, 45, 201, 77, 44, 208, 203, 47, 74, 215, 79, 44, 200, 212, 7, 26, 12, 0, 39, 119, 35, 71, 0, 0, 0, 11, 10, 7, 79, 83, 77, 68, 97, 116, 97, 24, 72, 16, 70, 26, 68, 120, 156, 227, 226, 227, 98, 224, 226, 114, 41, 205, 205, 173, 84, 8}, true},
	}

	for _, fl := range fls {
		t.Run(fl.Name, func(t *testing.T) {
			decoder := NewDecoder(bytes.NewReader(fl.Buf))
			err := decoder.Parse(newMockOSMReader())
			if fl.Err {
				assert.False(t, err == nil)
			} else {
				assert.True(t, err == nil)
			}
		})
	}
}

func BenchmarkReadBlock(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	assert.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(b, err)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		decoder := NewDecoder(reader)
		decoder.block()
	}
}

func BenchmarkReadMinimalFile(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	assert.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(b, err)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		or := newMockOSMReader()
		dec := NewDecoder(reader)
		err := dec.Parse(or)
		assert.Nil(b, err)
	}
}

func BenchmarkCompleteFile(b *testing.B) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		b.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}

	for i := 0; i < b.N; i++ {
		file, err := os.Open(testfile)
		assert.Nil(b, err)
		dec := NewDecoder(file)
		err = dec.Parse(newMockOSMReader())
		assert.Nil(b, err)
	}
}

func BenchmarkCompleteFileDecodeWithMetadata(b *testing.B) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		b.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}

	for i := 0; i < b.N; i++ {
		file, err := os.Open(testfile)
		assert.Nil(b, err)
		dec := NewDecoderWithInfo(file)
		err = dec.Parse(newMockOSMReader())
		assert.Nil(b, err)
	}
}

func BenchmarkStringTable(b *testing.B) {
	testFile, err := os.Open("testdata/stringtable.pbf")
	assert.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(b, err)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reader := bytes.NewBuffer(buf)
		or := newMockOSMReader()
		dec := NewDecoder(reader)
		b.StartTimer()
		dec.Parse(or)
	}
}
