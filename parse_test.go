package gosmparse

import (
	"fmt"
	"os"
	"testing"

	"github.com/aybabtme/iocontrol"
	"github.com/facebookgo/ensure"
	"github.com/qedus/osmpbf"
)

type mockOSMReader struct{}

func (r mockOSMReader) ReadNode(e *osmpbf.Node)         {}
func (r mockOSMReader) ReadWay(e *osmpbf.Way)           {}
func (r mockOSMReader) ReadRelation(e *osmpbf.Relation) {}

func TestParse(t *testing.T) {
	f, err := os.Open("bremen-latest.osm.pbf")
	ensure.Nil(t, err)
	defer f.Close()
	mr := iocontrol.NewMeasuredReader(f)

	err = Decode(mr, mockOSMReader{})
	fmt.Printf("Read %v Bytes/s, total read: %v Bytes\n", mr.BytesPerSec(), mr.Total())
	ensure.Nil(t, err)
}
