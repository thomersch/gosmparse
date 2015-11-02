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

	for _, pg := range pb.Primitivegroup {
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
