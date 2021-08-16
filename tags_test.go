package gosmparse

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(t, err)
	reader := bytes.NewReader(buf)

	dec := NewDecoder(reader)
	err = dec.Parse(&mr)
	assert.Nil(t, err)
	assert.Equal(t, mr.nodes, map[int64]map[string]string{
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
	assert.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(t, err)
	reader := bytes.NewReader(buf)

	dec := NewDecoder(reader)
	err = dec.Parse(&mr)
	assert.Nil(t, err)
	assert.Equal(t, mr.ways, map[int64]map[string]string{
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
	assert.Nil(t, err)
	buf, err := ioutil.ReadAll(testFile)
	assert.Nil(t, err)
	reader := bytes.NewReader(buf)

	dec := NewDecoder(reader)
	err = dec.Parse(&mr)
	assert.Nil(t, err)
	assert.Equal(t, mr.rels, map[int64]map[string]string{
		1: {"natural": "water", "wikipedia": "trololol"},
		2: {"unnatural": "water", "ref": "12", "name": "foobar"},
	})
}

func BenchmarkUnpackTags(b *testing.B) {
	const NumStrings int = 16

	var st []string
	for j := 0; j < NumStrings; j++ {
		st = append(st, fmt.Sprintf("abc%d", j))
	}

	// key, value .. key, value .. key, value
	var kv []int32
	for j := 0; j < NumStrings; j++ {
		// Increase the number of tags each object gets
		// as we go along
		for i := 0; i < j; i++ {
			kv = append(kv, int32((j+i)%NumStrings), int32((j+i+1)%NumStrings))
		}
		kv = append(kv, int32(0))
	}

	b.ResetTimer()
	// Each object will get its own unique key/value pairs
	for j := 0; j < b.N; j++ {
		var pos int = 0

		for pos < len(kv) {
			newPos, _ := unpackTags(st, pos, kv)
			if newPos == len(kv) {
				break
			}

			pos = newPos
		}
	}
}
