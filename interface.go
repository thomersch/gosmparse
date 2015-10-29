package gosmparse

import "github.com/qedus/osmpbf"

type OSMReader interface {
	ReadNode(Node)
	ReadWay(*osmpbf.Way)
	ReadRelation(*osmpbf.Relation)
}
