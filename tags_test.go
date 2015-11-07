package gosmparse

import (
	"bytes"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/facebookgo/ensure"
)

type mockedKVReader struct {
	sync.RWMutex

	nodes, ways, rels map[int64]map[string]string
}

func (r *mockedKVReader) ReadNode(n Node) {
	r.Lock()
	defer r.Unlock()
	r.nodes[n.ID] = n.Tags
}

func (r *mockedKVReader) ReadWay(w Way) {
	r.Lock()
	defer r.Unlock()
	r.ways[w.ID] = w.Tags
}

func (r *mockedKVReader) ReadRelation(rel Relation) {
	r.Lock()
	defer r.Unlock()
	r.rels[rel.ID] = rel.Tags
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

	err = Decode(reader, &mr)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, mr.nodes, map[int64]map[string]string{
		1: {"key1": "value1", "key2": "value2"},
		2: {"key2": "value2_node2"},
		3: {"key1": "value1_node3"},
	})
}

func TestWaysKV(t *testing.T) {
	mr := mockedKVReader{
		nodes: make(map[int64]map[string]string),
		ways:  make(map[int64]map[string]string),
	}

	testFile, err := os.Open("testdata/way_kv.osm.pbf")
	ensure.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(t, err)
	reader := bytes.NewReader(buf)

	err = Decode(reader, &mr)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, mr.ways, map[int64]map[string]string{
		1: {"name": "line", "highway": "primary"},
		2: {"highway": "primary", "foo": "bar"},
		3: {"unlogical": "true", "width": "3", "name": "line"},
	})
}

func TestRelationsKV(t *testing.T) {
	mr := mockedKVReader{
		nodes: make(map[int64]map[string]string),
		ways:  make(map[int64]map[string]string),
		rels:  make(map[int64]map[string]string),
	}

	testFile, err := os.Open("testdata/relation_kv.osm.pbf")
	ensure.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	ensure.Nil(t, err)
	reader := bytes.NewReader(buf)

	err = Decode(reader, &mr)
	ensure.Nil(t, err)
	ensure.DeepEqual(t, mr.rels, map[int64]map[string]string{
		1: {"natural": "water", "wikipedia": "trololol"},
		2: {"unnatural": "water", "ref": "12", "name": "foobar"},
	})
}
