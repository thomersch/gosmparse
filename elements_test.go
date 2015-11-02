package gosmparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
)

type mockedKVReader struct {
	sync.RWMutex

	nodes map[int64]map[string]string
}

func (r mockedKVReader) ReadNode(n Node) {
	r.Lock()
	defer r.Unlock()
	r.nodes[n.ID] = n.Tags
}

func (r mockedKVReader) ReadWay(w Way) {

}

func (r mockedKVReader) ReadRelation(rel Relation) {

}

func TestDenseNodeKV(t *testing.T) {
	mr := mockedKVReader{
		nodes: make(map[int64]map[string]string),
	}

	testFile, err := os.Open("testdata/node_kv.osm.pbf")
	ensure.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(t, err)
	reader := bytes.NewReader(buf)

	err = Decode(reader, mr)
	ensure.Nil(t, err)
	fmt.Println(mr.nodes)
}
