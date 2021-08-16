package gosmparse

import (
	"time"

	"github.com/thomersch/gosmparse/OSMPBF"
)

// Element contains common attributes of an OSM element (node/way/relation).
type Element struct {
	ID   int64
	Tags map[string]string

	// Info is only populated if you use NewDecoderWithInfo.
	Info *Info
}

// Node is an OSM data element with a position and tags (key/value pairs).
type Node struct {
	Element
	Lat float64
	Lon float64
}

// Way is an OSM data element that consists of Nodes and tags (key/value pairs).
// Ways can describe line strings or areas.
type Way struct {
	Element
	NodeIDs []int64
}

// Relation is an OSM data element that contains multiple elements (RelationMember)
// and has tags (key/value pairs).
type Relation struct {
	Element
	Members []RelationMember
}

// Info contains the metadata of an element.
type Info struct {
	Version   int
	Timestamp time.Time
	Changeset int64
	UID       int
	User      string
	Visible   bool
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

type denseState struct {
	DateGran             int64
	PosGran              int64
	LatOffset, LonOffset int64
	Strings              []string

	ID                    int64
	Lat, Lon              int64
	KVPos                 int
	OffTime, OffChangeset int64
	OffUserID, OffUser    int32
}

func denseNode(o OSMReader, pb *OSMPBF.PrimitiveBlock, dn *OSMPBF.DenseNodes, infoFn denseInfoFn) {
	ds := denseState{
		DateGran:  int64(pb.GetDateGranularity()),
		PosGran:   int64(pb.GetGranularity()),
		LatOffset: pb.GetLatOffset(),
		LonOffset: pb.GetLonOffset(),
		Strings:   pb.Stringtable.GetS(),
	}

	var n Node
	for index := range dn.Id {
		ds.ID += dn.Id[index]
		ds.Lat += dn.Lat[index]
		ds.Lon += dn.Lon[index]

		n.ID = ds.ID
		n.Lat = 1e-9 * float64(ds.LatOffset+(ds.PosGran*ds.Lat))
		n.Lon = 1e-9 * float64(ds.LonOffset+(ds.PosGran*ds.Lon))

		ds.KVPos, n.Tags = unpackTags(ds.Strings, ds.KVPos, dn.KeysVals)

		n.Info = infoFn(dn.Denseinfo, &ds, index)
		o.ReadNode(n)
	}
}

func way(o OSMReader, pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way, infoFn infoFn) error {
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
		w.Tags = make(map[string]string, len(way.Keys))
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

func relation(o OSMReader, pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation, infoFn infoFn) error {
	dateGran := int64(pb.GetDateGranularity())
	st := pb.Stringtable.GetS()

	var r Relation
	for _, rel := range relations {
		r.ID = *rel.Id
		r.Members = make([]RelationMember, len(rel.Memids))
		var (
			relMember RelationMember
			memID     int64
		)
		r.Tags = make(map[string]string, len(rel.Keys))
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
		r.Info = infoFn(rel.GetInfo(), dateGran, st)
		o.ReadRelation(r)
	}
	return nil
}

func denseInfo(i *OSMPBF.DenseInfo, ds *denseState, index int) *Info {
	ds.OffTime += i.Timestamp[index]
	ds.OffChangeset += i.Changeset[index]
	ds.OffUserID += i.Uid[index]
	ds.OffUser += i.UserSid[index]

	info := Info{
		Version:   int(i.Version[index]),
		Timestamp: time.Unix(ds.OffTime*ds.DateGran/1000, 0),
		Changeset: ds.OffChangeset,
		UID:       int(ds.OffUserID),
		User:      ds.Strings[ds.OffUser],
	}

	info.Visible = true
	if len(i.Visible) > index {
		info.Visible = i.Visible[index]
	}
	return &info
}

func info(i *OSMPBF.Info, gran int64, st []string) *Info {
	if i == nil {
		return nil
	}
	return &Info{
		Version:   int(i.GetVersion()),
		Timestamp: time.Unix(i.GetTimestamp()*gran/1000, 0),
		Changeset: i.GetChangeset(),
		UID:       int(i.GetUid()),
		User:      st[i.GetUserSid()],
		Visible:   i.GetVisible(),
	}
}
