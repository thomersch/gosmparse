package gosmparse

import (
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/thomersch/gosmparse/OSMPBF"
)

func Decode(r io.Reader, o OSMReader) error {
	dec := newDecoder()
	header, _, err := dec.Block(r)
	if err != nil {
		return err
	}
	// TODO: parser checks
	if header.GetType() != "OSMHeader" {
		return fmt.Errorf("Invalid header of first data block. Wanted: OSMHeader, have: %s", header.GetType())
	}

	// errChan := make(chan error)
	// feeder
	blobs := make(chan *OSMPBF.Blob, 200)
	go func() {
		defer close(blobs)
		for {
			_, blob, err := dec.Block(r)
			if err != nil {
				if err == io.EOF {
					return
				}
				// TODO: proper handling
				panic("error during parsing")
			}
			blobs <- blob
		}
	}()

	// processer
	numcpus := runtime.NumCPU() - 1
	if numcpus < 1 {
		numcpus = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < numcpus; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blob := range blobs {
				err := readElements(blob, dec, o)
				// TODO: proper error handling
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}

func readElements(blob *OSMPBF.Blob, dec *decoder, o OSMReader) error {
	pb, err := dec.BlobData(blob)
	if err != nil {
		return err
	}

	for _, pg := range pb.GetPrimitivegroup() {
		switch {
		case pg.Dense != nil:
			if err := denseNode(o, pb, pg.Dense); err != nil {
				return err
			}
		case len(pg.Ways) != 0:
			if err := way(o, pb, pg.Ways); err != nil {
				return err
			}
		case len(pg.Relations) != 0:
			if err := relation(o, pb, pg.Relations); err != nil {
				return err
			}
		case len(pg.Nodes) != 0:
			return fmt.Errorf("Nodes are not supported")
		default:
			return fmt.Errorf("no supported dat in primitive group")
		}
	}
	return nil
}

type Node struct {
	ID   int64
	Lat  float32
	Lon  float32
	Tags map[string]string
}

func denseNode(o OSMReader, pb *OSMPBF.PrimitiveBlock, dn *OSMPBF.DenseNodes) error {
	// TODO: implement key/value string table
	// st := pb.GetStringtable().GetS()
	// dateGran := pb.GetDateGranularity()
	gran := int64(pb.GetGranularity())
	latOffset := pb.GetLatOffset()
	lonOffset := pb.GetLonOffset()

	var (
		n            Node
		id, lat, lon int64
	)
	for index := range dn.Id {
		id = dn.Id[index] + id
		lat = dn.Lat[index] + lat
		lon = dn.Lon[index] + lon

		n.ID = id
		n.Lat = 1e-9 * float32(latOffset+(gran*lat))
		n.Lon = 1e-9 * float32(lonOffset+(gran*lon))
		// TODO: tags
		o.ReadNode(n)
	}
	return nil
}

type Way struct {
	ID      int64
	NodeIDs []int64
	Tags    map[string]string
}

func way(o OSMReader, pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way) error {
	// TODO: implement key/value string table
	// st := pb.GetStringtable().GetS()
	// dateGran := pb.GetDateGranularity()

	var (
		w      Way
		nodeID int64
	)
	for _, way := range ways {
		w.ID = way.GetId()
		nodeID = 0
		w.NodeIDs = make([]int64, len(way.Refs))
		for index := range way.Refs {
			nodeID = way.Refs[index] + nodeID
			w.NodeIDs[index] = nodeID
		}
		o.ReadWay(w)
	}
	return nil
}

type Relation struct {
	ID      int64
	Members []RelationMember
}

type MemberType int

const (
	NodeType MemberType = iota
	WayType
	RelationType
)

type RelationMember struct {
	ID   int64
	Type MemberType
	Role string
}

func relation(o OSMReader, pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation) error {
	// TODO: implement key/value string table
	st := pb.Stringtable.S
	// dateGran := pb.GetDateGranularity()
	var r Relation
	for _, rel := range relations {
		r.ID = *rel.Id
		r.Members = make([]RelationMember, len(rel.Memids))
		var (
			relMember RelationMember
			memID     int64
		)
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
		}
		o.ReadRelation(r)
	}
	return nil
}
