package gosmparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thomersch/gosmparse/OSMPBF"

	"github.com/aybabtme/iocontrol"
	"github.com/dustin/go-humanize"
	"github.com/facebookgo/ensure"
	"github.com/gogo/protobuf/proto"
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
	ensure.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	rdr := newMockOSMReader()
	d := NewDecoder(mr)
	err = d.Parse(rdr)
	fmt.Printf("Speed: %v/s, total read: %v\n", humanize.Bytes(mr.BytesPerSec()), humanize.Bytes(uint64(mr.Total())))
	fmt.Printf("Read %v nodes, %v ways, %v relations\n", atomic.LoadUint64(rdr.Nodes), atomic.LoadUint64(rdr.Ways), atomic.LoadUint64(rdr.Relations))
	ensure.Nil(t, err)
}

func TestMinimalParse(t *testing.T) {
	testFile, err := os.Open("testdata/relation.pbf")
	ensure.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(t, err)

	reader := bytes.NewReader(buf)
	or := &cachedReader{}
	dec := NewDecoder(reader)
	err = dec.Parse(or)
	ensure.Nil(t, err)

	ensure.DeepEqual(t, or.Rels[0].Members[0].ID, int64(15))
	ensure.DeepEqual(t, or.Rels[0].Members[0].ID, int64(15))
	ensure.DeepEqual(t, or.Rels[0].Members[1].ID, int64(16))
	ensure.DeepEqual(t, or.Rels[0].Members[2].ID, int64(17))
	ensure.DeepEqual(t, or.Rels[0].Members[3].ID, int64(20))
	ensure.DeepEqual(t, or.Rels[0].Members[4].ID, int64(100))
	ensure.DeepEqual(t, or.Rels[0].Members[5].ID, int64(101))
	ensure.DeepEqual(t, or.Rels[0].Members[6].ID, int64(102))
	ensure.DeepEqual(t, or.Rels[0].Members[7].ID, int64(98))
}

func TestParseHistory(t *testing.T) {
	testFile, err := os.Open("testdata/history.osh.pbf")
	ensure.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(t, err)

	reader := bytes.NewReader(buf)
	or := &cachedReader{}
	dec := NewFullDecoder(reader)
	err = dec.Parse(or)
	ensure.Nil(t, err)

	ensure.DeepEqual(t, or.Nodes[0].Lat, float64(0.001))
	ensure.DeepEqual(t, or.Nodes[0].Info.Visible, true)
	ensure.DeepEqual(t, or.Nodes[0].Info.Timestamp, int64(1446404400000))
	ensure.DeepEqual(t, or.Nodes[1].Info.UID, int32(1))
	ensure.DeepEqual(t, or.Nodes[1].Info.Changeset, int64(1))
	ensure.DeepEqual(t, or.Nodes[2].Lat, float64(0.003))
	ensure.DeepEqual(t, or.Nodes[2].Info.Timestamp, int64(1554145200000))
	ensure.DeepEqual(t, or.Nodes[3].Info.Visible, false)

	ensure.DeepEqual(t, or.Ways[0].Info.Changeset, int64(1))
	ensure.DeepEqual(t, or.Ways[1].Info.Changeset, int64(2))
	ensure.DeepEqual(t, or.Ways[1].Info.User, "Another User")

	ensure.DeepEqual(t, or.Rels[0].Members[0].ID, int64(1))
	ensure.DeepEqual(t, or.Rels[0].Members[0].Type, NodeType)
	ensure.DeepEqual(t, or.Rels[0].Members[1].ID, int64(1))
	ensure.DeepEqual(t, or.Rels[0].Members[1].Type, WayType)
	ensure.DeepEqual(t, or.Rels[0].Info.Visible, true)
	ensure.DeepEqual(t, or.Rels[0].Info.User, "Dummy User")
	ensure.DeepEqual(t, or.Rels[1].Info.Visible, false)
	ensure.DeepEqual(t, or.Rels[1].Info.UID, int32(2))
}

func TestBlobDataUncompressed(t *testing.T) {
	originalPrimBlock := &OSMPBF.PrimitiveBlock{
		Stringtable: &OSMPBF.StringTable{},
	}
	primitiveBlockBytes, err := proto.Marshal(originalPrimBlock)
	ensure.Nil(t, err)
	blob := &OSMPBF.Blob{
		Raw: primitiveBlockBytes,
	}

	d := NewDecoder(nil)
	primBlock, err := d.blobData(blob)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, primBlock, originalPrimBlock)
}

func BenchmarkReadBlock(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	ensure.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(b, err)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		decoder := NewDecoder(reader)
		decoder.block()
	}
}

func BenchmarkReadMinimalFile(b *testing.B) {
	testFile, err := os.Open("testdata/base.pbf")
	ensure.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(b, err)
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(buf)
		or := newMockOSMReader()
		dec := NewDecoder(reader)
		err := dec.Parse(or)
		ensure.Nil(b, err)
	}
}

func BenchmarkCompleteFile(b *testing.B) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		b.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}

	for i := 0; i < b.N; i++ {
		file, err := os.Open(testfile)
		ensure.Nil(b, err)
		dec := NewDecoder(file)
		err = dec.Parse(newMockOSMReader())
		ensure.Nil(b, err)
	}
}

func BenchmarkCompleteFileFullDecode(b *testing.B) {
	testfile := os.Getenv("TESTFILE")
	if testfile == "" {
		b.Skip("No testfile specified. Please set `TESTFILE` environment variable with the file path.")
	}

	for i := 0; i < b.N; i++ {
		file, err := os.Open(testfile)
		ensure.Nil(b, err)
		dec := NewFullDecoder(file)
		err = dec.Parse(newMockOSMReader())
		ensure.Nil(b, err)
	}
}

func BenchmarkStringTable(b *testing.B) {
	testFile, err := os.Open("testdata/stringtable.pbf")
	ensure.Nil(b, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(b, err)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		reader := bytes.NewBuffer(buf)
		or := newMockOSMReader()
		dec := NewDecoder(reader)
		b.StartTimer()
		dec.Parse(or)
	}
}
