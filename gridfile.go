package mgo

import (
	"errors"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"io"
)

type gfsFileMode int

const (
	gfsClosed  gfsFileMode = 0
	gfsReading gfsFileMode = 1
	gfsWriting gfsFileMode = 2
)

type GridFile struct {
	gfs      *GridFS
	mode     gfsFileMode
	fileName string
	stream   io.Closer
}

func (f *GridFile) Read(p []byte) (n int, err error) {
	f.assertMode(gfsReading)

	if reader, ok := f.stream.(io.ReadCloser); ok {
		return reader.Read(p)
	}
	return -1, errors.New("Mode Error")
}
func (file *GridFile) assertMode(mode gfsFileMode) {
	switch file.mode {
	case mode:
		return
	case gfsWriting:
		panic("GridFile is open for writing")
	case gfsReading:
		panic("GridFile is open for reading")
	case gfsClosed:
		panic("GridFile is closed")
	default:
		panic("internal error: missing GridFile mode")
	}
}

func newGridFile(gfs *GridFS, fileName string) (file *GridFile) {
	return &GridFile{gfs: gfs, fileName: fileName}
}

func (f *GridFile) Write(content []byte) (int, error) {
	f.assertMode(gfsWriting)
	stream, err := f.gfs.CreateStream(f.fileName)
	if err != nil {
		return -1, err
	}

	return stream.Write(content)
}

func (f *GridFile) Close() error {
	return f.stream.Close()
}

func (f *GridFile) Id() interface{} {
	f.assertMode(gfsReading)
	return f.stream.(*gridfs.UploadStream).FileID
}
