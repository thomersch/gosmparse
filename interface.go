package gosmparse

// OSMReader is the interface that needs to be implemented in order to receive
// Elements from the parsing process.
type OSMReader interface {
	ReadNode(Node)
	ReadWay(Way)
	ReadRelation(Relation)
}

type OSMBatchedReader interface {
	ReadNodes([]Node)
	ReadWays([]Way)
	ReadRelations([]Relation)
}

type batchedToSingleReader struct {
	SR OSMReader
}

func (bsr *batchedToSingleReader) ReadNodes(nds []Node) {
	for _, nd := range nds {
		bsr.SR.ReadNode(nd)
	}
}

func (bsr *batchedToSingleReader) ReadWays(ways []Way) {
	for _, way := range ways {
		bsr.SR.ReadWay(way)
	}
}

func (bsr *batchedToSingleReader) ReadRelations(rels []Relation) {
	for _, rel := range rels {
		bsr.SR.ReadRelation(rel)
	}
}
