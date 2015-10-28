package gosmparse

import (
	"fmt"
	"io"
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

	var wg sync.WaitGroup
	for {
		_, blob, err := readBlock(r, dec)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err = dec.BlobData(blob)
			// TODO: proper error handling
			if err != nil {
				panic(err)
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
