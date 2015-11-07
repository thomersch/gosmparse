package gosmparse

// OSMReader is the interface that needs to be implemented in order to receive
// Elements from the parsing process.
type OSMReader interface {
	ReadNode(Node)
	ReadWay(Way)
	ReadRelation(Relation)
}
