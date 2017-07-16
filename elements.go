package gosmparse

import "github.com/thomersch/gosmparse/OSMPBF"

// Node is an OSM data element with a position and tags (key/value pairs).
type Node struct {
	ID   int64
	Lat  float64
	Lon  float64
	Tags map[string]string
}

// Way is an OSM data element that consists of Nodes and tags (key/value pairs).
// Ways can describe line strings or areas.
type Way struct {
	ID      int64
	NodeIDs []int64
	Tags    map[string]string
}

// Relation is an OSM data element that contains multiple elements (RelationMember)
// and has tags (key/value pairs).
type Relation struct {
	ID      int64
	Members []RelationMember
	Tags    map[string]string
}

// MemberType describes the type of a relation member (node/way/relation).
type MemberType int

const (
	NodeType MemberType = iota
	WayType
	RelationType
)

// RelationMember refers to an element in a relation. It contains the ID of the element
// (node/way/relation) and the role.
type RelationMember struct {
	ID   int64
	Type MemberType
	Role string
}

func denseNode(o OSMReader, pb *OSMPBF.PrimitiveBlock, dn *OSMPBF.DenseNodes) error {
	// dateGran := pb.GetDateGranularity()
	gran := int64(pb.GetGranularity())
	latOffset := pb.GetLatOffset()
	lonOffset := pb.GetLonOffset()

	var (
		n            Node
		id, lat, lon int64
		kvPos        int // position in kv slice
	)
	for index := range dn.Id {
		id = dn.Id[index] + id
		lat = dn.Lat[index] + lat
		lon = dn.Lon[index] + lon

		n.ID = id
		n.Lat = 1e-9 * float64(latOffset+(gran*lat))
		n.Lon = 1e-9 * float64(lonOffset+(gran*lon))

		kvPos, n.Tags = unpackTags(pb.Stringtable.GetS(), kvPos, dn.KeysVals)
		o.ReadNode(n)
	}
	return nil
}

func way(o OSMReader, pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way) error {
	// dateGran := pb.GetDateGranularity()
	st := pb.Stringtable.GetS()

	var (
		w      Way
		nodeID int64
	)
	for _, way := range ways {
		w.ID = way.GetId()
		nodeID = 0
		w.NodeIDs = make([]int64, len(way.Refs))
		w.Tags = make(map[string]string)
		for pos, key := range way.Keys {
			keyString := string(st[int(key)])
			w.Tags[keyString] = string(st[way.Vals[pos]])
		}
		for index := range way.Refs {
			nodeID = way.Refs[index] + nodeID
			w.NodeIDs[index] = nodeID
		}
		o.ReadWay(w)
	}
	return nil
}

func relation(o OSMReader, pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation) error {
	st := pb.Stringtable.GetS()
	// dateGran := pb.GetDateGranularity()
	var r Relation
	for _, rel := range relations {
		r.ID = *rel.Id
		r.Members = make([]RelationMember, len(rel.Memids))
		var (
			relMember RelationMember
			memID     int64
		)
		r.Tags = make(map[string]string)
		for pos, key := range rel.Keys {
			keyString := string(st[int(key)])
			r.Tags[keyString] = string(st[rel.Vals[pos]])
		}
		for memIndex := range rel.Memids {
			memID = rel.Memids[memIndex] + memID
			relMember.ID = memID
			switch rel.Types[memIndex] {
			case OSMPBF.Relation_NODE:
				relMember.Type = NodeType
			case OSMPBF.Relation_WAY:
				relMember.Type = WayType
			case OSMPBF.Relation_RELATION:
				relMember.Type = RelationType
			}
			relMember.Role = string(st[rel.RolesSid[memIndex]])
			r.Members[memIndex] = relMember
		}
		o.ReadRelation(r)
	}
	return nil
}
