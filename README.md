# OpenStreetMap PBF Parser in Go

[![GoDoc](https://godoc.org/github.com/thomersch/gosmparse?status.svg)](https://godoc.org/github.com/thomersch/gosmparse) [![Build Status](https://travis-ci.org/thomersch/gosmparse.svg?branch=master)](https://travis-ci.org/thomersch/gosmparse)

Gosmparse works already, but the API may change ([Documentation](https://godoc.org/github.com/thomersch/gosmparse)).

It has been designed with performance and maximum usage convenience in mind; on an Intel Core i7-6820HQ with NVMe flash it is able to process 67 MB/s, so a planet file can be processed in less than 10 minutes. If you find possible speed-ups or other improvements, let me know.

## Characteristics

* fast
* `panic`-free
* tested with different files from different sources/generators
* more than 80% test coverage and has benchmarks for all hot spots
* one dependency only: [Go protobuf package](http://github.com/golang/protobuf)
* can read from any io.Reader (e.g. for parsing during download)

## Install

```
go get -u github.com/thomersch/gosmparse
```

## Example Usage

```go
// Implement the gosmparser.OSMReader interface here.
// Streaming data will call those functions.
type dataHandler struct{}

func (d *dataHandler) ReadNode(n gosmparse.Node)         {}
func (d *dataHandler) ReadWay(w gosmparse.Way)           {}
func (d *dataHandler) ReadRelation(r gosmparse.Relation) {}

func ExampleNewDecoder() {
	r, err := os.Open("filename.pbf")
	if err != nil {
		panic(err)
	}
	dec := gosmparse.NewDecoder(r)
	// Parse will block until it is done or an error occurs.
	err = dec.Parse(&dataHandler{})
	if err != nil {
		panic(err)
	}
}
```

## Download & Parse

It is possible to parse during download, so you don't have to wait for a download to finish to be able to start the parsing/processing. You can simply use the standard Go `net/http` package and pass `resp.Body` to the decoder.

```go
resp, err := http.Get("http://download.geofabrik.de/europe/germany/bremen-latest.osm.pbf")
if err != nil {
	panic(err)
}
defer resp.Body.Close()
dec := gosmparse.NewDecoder(resp.Body)
err = dec.Parse(&dataHandler{})
if err != nil {
	panic(err)
}
```

## Did it break?

If you found a case, where gosmparse broke, please report it and provide the file that caused the failure.
