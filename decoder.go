package gosmparse

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/thomersch/gosmparse/OSMPBF"

	"github.com/golang/protobuf/proto"
)

// A Decoder reads and decodes OSM data from an input stream.
type Decoder struct {
	// QueueSize allows to tune the memory usage vs. parse speed.
	// A larger QueueSize will consume more memory, but may speed up the parsing process.
	QueueSize int
	r         io.Reader
	o         OSMReader
}

// NewDecoder returns a new decoder that reads from r.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:         r,
		QueueSize: 200,
	}
}

// Parse starts the parsing process that will stream data into the given OSMReader.
func (d *Decoder) Parse(o OSMReader) error {
	d.o = o
	header, _, err := d.block()
	if err != nil {
		return err
	}
	// TODO: parser checks
	if header.GetType() != "OSMHeader" {
		return fmt.Errorf("Invalid header of first data block. Wanted: OSMHeader, have: %s", header.GetType())
	}

	errChan := make(chan error)
	// feeder
	blobs := make(chan *OSMPBF.Blob, d.QueueSize)
	go func() {
		defer close(blobs)
		for {
			_, blob, err := d.block()
			if err != nil {
				if err == io.EOF {
					return
				}
				errChan <- err
				return
			}
			blobs <- blob
		}
	}()

	consumerCount := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blob := range blobs {
				err := d.readElements(blob)
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	finished := make(chan bool)
	go func() {
		wg.Wait()
		finished <- true
	}()
	select {
	case err = <-errChan:
		return err
	case <-finished:
		return nil
	}
}

func (d *Decoder) block() (*OSMPBF.BlobHeader, *OSMPBF.Blob, error) {
	// BlobHeaderLength
	headerSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(d.r, headerSizeBuf); err != nil {
		return nil, nil, err
	}
	headerSize := binary.BigEndian.Uint32(headerSizeBuf)

	// BlobHeader
	headerBuf := make([]byte, headerSize)
	if _, err := io.ReadFull(d.r, headerBuf); err != nil {
		return nil, nil, err
	}
	blobHeader := new(OSMPBF.BlobHeader)
	if err := proto.Unmarshal(headerBuf, blobHeader); err != nil {
		return nil, nil, err
	}

	// Blob
	blobBuf := make([]byte, blobHeader.GetDatasize())
	_, err := io.ReadFull(d.r, blobBuf)
	if err != nil {
		return nil, nil, err
	}
	blob := new(OSMPBF.Blob)
	if err := proto.Unmarshal(blobBuf, blob); err != nil {
		return nil, nil, err
	}
	return blobHeader, blob, nil
}

func (d *Decoder) readElements(blob *OSMPBF.Blob) error {
	pb, err := d.blobData(blob)
	if err != nil {
		return err
	}

	for _, pg := range pb.Primitivegroup {
		switch {
		case pg.Dense != nil:
			if err := denseNode(d.o, pb, pg.Dense); err != nil {
				return err
			}
		case len(pg.Ways) != 0:
			if err := way(d.o, pb, pg.Ways); err != nil {
				return err
			}
		case len(pg.Relations) != 0:
			if err := relation(d.o, pb, pg.Relations); err != nil {
				return err
			}
		case len(pg.Nodes) != 0:
			return fmt.Errorf("Nodes are not supported")
		default:
			return fmt.Errorf("no supported data in primitive group")
		}
	}
	return nil
}

// should be concurrency safe
func (d *Decoder) blobData(blob *OSMPBF.Blob) (*OSMPBF.PrimitiveBlock, error) {
	buf := make([]byte, blob.GetRawSize())
	switch {
	case blob.Raw != nil:
		buf = blob.Raw
	case blob.ZlibData != nil:
		r, err := zlib.NewReader(bytes.NewReader(blob.GetZlibData()))
		if err != nil {
			return nil, err
		}
		defer r.Close()

		n, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}
		if n != int(blob.GetRawSize()) {
			return nil, fmt.Errorf("expected %v bytes, read %v", blob.GetRawSize(), n)
		}
	default:
		return nil, fmt.Errorf("found block with unknown data")
	}
	var primitiveBlock = OSMPBF.PrimitiveBlock{}
	err := proto.Unmarshal(buf, &primitiveBlock)
	return &primitiveBlock, err
}
