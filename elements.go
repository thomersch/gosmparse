package gosmparse

import (
	"github.com/thomersch/gosmparse/OSMPBF"
)

// Info is optional metadata includes non-geographic information about an element
type Info struct {
	Version   int32
	Timestamp int64
	Changeset int64
	UID       int32
	User      string
	Visible   bool
}

// Node is an OSM data element with a position and tags (key/value pairs).
type Node struct {
	ID   int64
	Lat  float64
	Lon  float64
	Tags map[string]string
	Info *Info
}

// Way is an OSM data element that consists of Nodes and tags (key/value pairs).
// Ways can describe line strings or areas.
type Way struct {
	ID      int64
	NodeIDs []int64
	Tags    map[string]string
	Info    *Info
}

// Relation is an OSM data element that contains multiple elements (RelationMember)
// and has tags (key/value pairs).
type Relation struct {
	ID      int64
	Members []RelationMember
	Tags    map[string]string
	Info    *Info
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
	dateGran := int64(pb.GetDateGranularity())
	gran := int64(pb.GetGranularity())
	latOffset := pb.GetLatOffset()
	lonOffset := pb.GetLonOffset()
	st := pb.Stringtable.GetS()

	var (
		n            Node
		id, lat, lon int64
		kvPos        int // position in kv slice
		ts, cs       int64
		uid, usid    int32
	)
	for index := range dn.Id {
		id = dn.Id[index] + id
		lat = dn.Lat[index] + lat
		lon = dn.Lon[index] + lon

		n.ID = id
		n.Lat = 1e-9 * float64(latOffset+(gran*lat))
		n.Lon = 1e-9 * float64(lonOffset+(gran*lon))

		kvPos, n.Tags = unpackTags(st, kvPos, dn.KeysVals)

		if dn.Denseinfo != nil {
			ts = dn.Denseinfo.Timestamp[index] + ts
			cs = dn.Denseinfo.Changeset[index] + cs
			uid = dn.Denseinfo.Uid[index] + uid
			usid = dn.Denseinfo.UserSid[index] + usid

			visible := true
			if len(dn.Denseinfo.Visible) > index {
				visible = dn.Denseinfo.Visible[index]
			}
			n.Info = &Info{
				Version:   dn.Denseinfo.Version[index],
				Timestamp: ts * dateGran,
				Changeset: cs,
				UID:       uid,
				User:      st[usid],
				Visible:   visible,
			}
		}

		o.ReadNode(n)
	}
	return nil
}

func info(i *OSMPBF.Info, gran int64, st []string) *Info {
	if i == nil {
		return nil
	}
	return &Info{
		Version:   i.GetVersion(),
		Timestamp: i.GetTimestamp() * gran,
		Changeset: i.GetChangeset(),
		UID:       i.GetUid(),
		User:      st[i.GetUserSid()],
		Visible:   i.GetVisible(),
	}
}

func way(o OSMReader, pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way) error {
	dateGran := int64(pb.GetDateGranularity())
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
		w.Info = info(way.GetInfo(), dateGran, st)
		o.ReadWay(w)
	}
	return nil
}

func relation(o OSMReader, pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation) error {
	dateGran := int64(pb.GetDateGranularity())
	st := pb.Stringtable.GetS()

	var r Relation
	for _, rel := range relations {
		r.ID = rel.Id
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
		r.Info = info(rel.GetInfo(), dateGran, st)
		o.ReadRelation(r)
	}
	return nil
}
