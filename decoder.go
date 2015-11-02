package gosmparse

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/thomersch/gosmparse/OSMPBF"

	"github.com/golang/protobuf/proto"
)

type decoder struct{}

func newDecoder() *decoder {
	return &decoder{}
}

func (d *decoder) Block(r io.Reader) (*OSMPBF.BlobHeader, *OSMPBF.Blob, error) {
	// BlobHeaderLength
	headerSizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, headerSizeBuf); err != nil {
		return nil, nil, err
	}
	headerSize := binary.BigEndian.Uint32(headerSizeBuf)

	// BlobHeader
	headerBuf := make([]byte, headerSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, nil, err
	}
	blobHeader := new(OSMPBF.BlobHeader)
	if err := proto.Unmarshal(headerBuf, blobHeader); err != nil {
		return nil, nil, err
	}

	// Blob
	blobBuf := make([]byte, blobHeader.GetDatasize())
	_, err := io.ReadFull(r, blobBuf)
	if err != nil {
		return nil, nil, err
	}
	blob := new(OSMPBF.Blob)
	if err := proto.Unmarshal(blobBuf, blob); err != nil {
		return nil, nil, err
	}
	return blobHeader, blob, nil
}

// should be concurrency safe
func (d *decoder) BlobData(blob *OSMPBF.Blob) (*OSMPBF.PrimitiveBlock, error) {
	buf := make([]byte, blob.GetRawSize())
	switch {
	case blob.Raw != nil:
		return nil, fmt.Errorf("Raw data is not supported.")
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
