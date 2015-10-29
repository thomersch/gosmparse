package gosmparse

import (
	"fmt"
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
	f, err := os.Open("niedersachsen-latest.osm.pbf")
	ensure.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	rdr := mockOSMReader{Nodes: new(uint64), Ways: new(uint64), Relations: new(uint64)}
	err = Decode(mr, rdr)
	fmt.Printf("Speed: %v/s, total read: %v\n", humanize.Bytes(mr.BytesPerSec()), humanize.Bytes(uint64(mr.Total())))
	fmt.Printf("Read %v nodes, %v ways, %v relations\n", atomic.LoadUint64(rdr.Nodes), atomic.LoadUint64(rdr.Ways), atomic.LoadUint64(rdr.Relations))
	ensure.Nil(t, err)
}
