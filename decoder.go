package gosmparse

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/qedus/osmpbf/OSMPBF"
)

type decoder struct {
	headerBuf []byte
}

func newDecoder() *decoder {
	return &decoder{
		headerBuf: make([]byte, 4),
	}
}

func (d *decoder) HeaderSize(r io.Reader) (uint32, error) {
	if _, err := io.ReadFull(r, d.headerBuf); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(d.headerBuf), nil
}

func (d *decoder) BlobHeader(r io.Reader, size uint32) (*OSMPBF.BlobHeader, error) {
	fmt.Printf("BlobHeader Size: %v\n", size)
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	blobHeader := new(OSMPBF.BlobHeader)
	if err := proto.Unmarshal(buf, blobHeader); err != nil {
		return nil, err
	}
	return blobHeader, nil
}
