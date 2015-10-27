package gosmparse

import (
	"os"
	"testing"

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

	err = Decode(f, mockOSMReader{})
	ensure.Nil(t, err)
}
