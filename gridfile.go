package mgo

import (
	"go.mongodb.org/mongo-driver/mongo/gridfs"
)

type gfsFileMode int

const (
	gfsClosed  gfsFileMode = 0
	gfsReading gfsFileMode = 1
	gfsWriting gfsFileMode = 2
)

type GridFile struct {
	gfs            *GridFS
	mode           gfsFileMode
	fileName       string
	fileSize       int64
	uploadStream   *gridfs.UploadStream
	downloadStream *gridfs.DownloadStream
	fileId         interface{}
}

func (gf *GridFile) Read(p []byte) (n int, err error) {
	gf.assertMode(gfsReading)

	return gf.downloadStream.Read(p)
}
func (gf *GridFile) assertMode(mode gfsFileMode) {
	switch gf.mode {
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

func newGridFile(gfs *GridFS, fileName string, mode gfsFileMode) (file *GridFile, err error) {
	gfsFile := &GridFile{gfs: gfs, fileName: fileName, mode: gfsReading}

	switch mode {
	case gfsReading:
		ds, err := gfs.bucket.OpenDownloadStreamByName(fileName)
		if err != nil {
			return nil, err
		}

		gfsFile.downloadStream = ds
		gfsFile.fileId = ds.GetFile().ID
		gfsFile.fileSize = ds.GetFile().Length
	case gfsWriting:
		us, err := gfs.bucket.OpenUploadStream(fileName)
		if err != nil {
			return nil, err
		}
		gfsFile.uploadStream = us
		gfsFile.fileId = us.FileID
		gfsFile.fileSize = 0
	default:
		panic("gfsfile:mode not allowed")
	}
	return gfsFile, err
}

func (gf *GridFile) Write(content []byte) (int, error) {
	gf.assertMode(gfsWriting)
	n, err := gf.uploadStream.Write(content)
	if err != nil {
		return -1, err
	}
	gf.fileSize += int64(n)
	return n, nil
}

func (gf *GridFile) Close() (err error) {
	if gf.uploadStream != nil {
		err = gf.uploadStream.Close()
	}
	if gf.downloadStream != nil {
		err = gf.downloadStream.Close()
	}
	return err
}

func (gf *GridFile) Id() interface{} {

	return gf.fileId
}

func (gf *GridFile) Size() (bytes int64) {
	return gf.fileSize
}

func (gf *GridFile) Name() string {
	return gf.fileName
}
