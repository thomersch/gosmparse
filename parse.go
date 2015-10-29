package gosmparse

import (
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/qedus/osmpbf/OSMPBF"
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
		return nil, nil, err
	}
	blob, err := dec.Blob(r, blobHeader)
	if err != nil {
		return nil, nil, err
	}
	return blobHeader, blob, nil
}

func readElements(blob *OSMPBF.Blob, dec *decoder, o OSMReader) error {
	// tStart := time.Now()
	pb, err := dec.BlobData(blob)
	if err != nil {
		return err
	}
	// fmt.Printf("Blob Data read: %v\n", time.Now().Sub(tStart))

	for _, pg := range pb.GetPrimitivegroup() {
		switch {
		case pg.Dense != nil:
			return denseNode(o, pb, pg.GetDense())
		case pg.Ways != nil:

		case pg.Relations != nil:

		default:
			return fmt.Errorf("unkown data type")
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
