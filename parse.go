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
	header, _, err := readBlock(r, dec)
	if err != nil {
		return err
	}
	// TODO: parser checks
	if header.GetType() != "OSMHeader" {
		return fmt.Errorf("Invalid header of first data block. Wanted: OSMHeader, have: %s", header.GetType())
	}

	// feeder
	blobs := make(chan *OSMPBF.Blob, 200)
	go func() {
		defer close(blobs)
		for {
			_, blob, err := readBlock(r, dec)
			if err != nil {
				if err == io.EOF {
					fmt.Println("Reading finished")
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

func readBlock(r io.Reader, dec *decoder) (*OSMPBF.BlobHeader, *OSMPBF.Blob, error) {
	// read file block
	size, err := dec.HeaderSize(r)
	if err != nil {
		return nil, nil, err
	}
	blobHeader, err := dec.BlobHeader(r, size)
	if err != nil {
		fmt.Println(73)
		return nil, nil, err
	}
	blob, err := dec.Blob(r, blobHeader)
	if err != nil {
		return nil, nil, err
	}
	return blobHeader, blob, nil
}

func readElements(blob *OSMPBF.Blob, dec *decoder, o OSMReader) error {
	pb, err := dec.BlobData(blob)
	if err != nil {
		return err
	}

	for _, pg := range pb.Primitivegroup {
		switch {
		case pg.Nodes != nil:
			return fmt.Errorf("Nodes are not supported")
		case pg.Dense != nil:
			return denseNode(o, pb, pg.Dense)
		case pg.Ways != nil:
			return way(o, pb, pg.Ways)
		case pg.Relations != nil:
			return relation(o, pb, pg.Relations)
		default:
			return fmt.Errorf("unknown data type")
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
