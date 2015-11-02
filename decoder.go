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

type decoder struct {
	headerBuf, blobHeaderBuf []byte
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

// BlobHeader is not concurrency safe.
func (d *decoder) BlobHeader(r io.Reader, size uint32) (*OSMPBF.BlobHeader, error) {
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

func (d *decoder) Blob(r io.Reader, blobHeader *OSMPBF.BlobHeader) (*OSMPBF.Blob, error) {
	datasize := blobHeader.GetDatasize()
	// TODO: share buffer, if always/often the same size
	buf := make([]byte, datasize)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	blob := new(OSMPBF.Blob)
	if err := proto.Unmarshal(buf, blob); err != nil {
		return nil, err
	}
	return blob, nil
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
