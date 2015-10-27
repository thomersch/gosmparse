package gosmparse

import "io"

func Decode(r io.Reader, o OSMReader) error {
	dec := newDecoder()
	// read file block
	size, err := dec.HeaderSize(r)
	if err != nil {
		return err
	}
	_, err = dec.BlobHeader(r, size)
	if err != nil {
		return err
	}

	return nil
}
