package gosmparse

type OSMReader interface {
	ReadNode(Node)
	ReadWay(Way)
	ReadRelation(Relation)
}
