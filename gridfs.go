package mgo

import (
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
)

type GridFS struct {
	bucket   *gridfs.Bucket
	fileColl *Collection
	fileName string
}

func (g *GridFS) CreateStream(name string) (*gridfs.UploadStream, error) {
	return g.bucket.OpenUploadStream(name)
}
func (g *GridFS) Create(name string) (file *GridFile, err error) {
	us, err := g.CreateStream(name)
	if err != nil {
		return
	}
	return &GridFile{
		gfs:          g,
		mode:         gfsWriting,
		fileId:       us.FileID,
		fileName:     name,
		uploadStream: us,
	}, nil
}

type gfsDocId struct {
	Id bson.ObjectId `bson:"_id"`
}

func (g *GridFS) Remove(name string) (err error) {
	iter := g.Find(bson.M{"filename": name}).Select(bson.M{"_id": 1}).Iter()
	var doc gfsDocId
	for iter.Next(&doc) {
		if err1 := g.RemoveId(doc.Id); err1 != nil {
			err = err1
		}
	}
	return err

}
func (g *GridFS) RemoveId(id interface{}) (err error) {
	return g.bucket.Delete(queryID(id))
}
func (g *GridFS) Find(query interface{}) *Query {
	return g.fileColl.Find(query)
}

func (g *GridFS) OpenStreamId(id bson.ObjectId) (*gridfs.DownloadStream, error) {
	return g.bucket.OpenDownloadStream(id)
}

func (g *GridFS) Close() {

}
func (g *GridFS) Open(name string) (file *GridFile, err error) {

	file, err = newGridFile(g, name, gfsReading)
	if err == gridfs.ErrFileNotFound {
		err = ErrNotFound
	}
	return
}
func (g *GridFS) OpenId(id bson.ObjectId) (file *GridFile, err error) {
	stream, err := g.bucket.OpenDownloadStream(id)
	if err != nil {
		if err == gridfs.ErrFileNotFound {
			err = ErrNotFound
		}
		return
	}
	return &GridFile{
		gfs:            g,
		mode:           gfsReading,
		downloadStream: stream,
	}, nil
}
