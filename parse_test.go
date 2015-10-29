package gosmparse

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aybabtme/iocontrol"
	"github.com/dustin/go-humanize"
	"github.com/facebookgo/ensure"
	"github.com/qedus/osmpbf"
)

type mockOSMReader struct {
	Nodes *uint64
}

func (r mockOSMReader) ReadNode(n Node) {
	atomic.AddUint64(r.Nodes, 1)
}

func (r mockOSMReader) ReadWay(e *osmpbf.Way)           {}
func (r mockOSMReader) ReadRelation(e *osmpbf.Relation) {}

func TestParse(t *testing.T) {
	f, err := os.Open("bremen-latest.osm.pbf")
	ensure.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	rdr := mockOSMReader{Nodes: new(uint64)}
	err = Decode(mr, rdr)
	fmt.Printf("End: %v\n", atomic.LoadUint64(rdr.Nodes))
	fmt.Printf("Read %v/s, total read: %v\n", humanize.Bytes(mr.BytesPerSec()), humanize.Bytes(uint64(mr.Total())))
	fmt.Printf("Read %v nodes\n", atomic.LoadUint64(rdr.Nodes))
	ensure.Nil(t, err)
}
