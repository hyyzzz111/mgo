package mgo

import (
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"github.com/yaziming/mgo/bson"
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
	return &GridFile{
		gfs:      g,
		mode: gfsWriting,
		fileName: name,
	},nil
}
func (g *GridFS) RemoveId(name interface{}) error {
	return g.bucket.Delete(name)
}
func (g *GridFS) Find(query interface{}) *Query {
	return g.fileColl.Find(query)
}

func (g *GridFS) OpenStreamId(id bson.ObjectId) (*gridfs.DownloadStream, error) {
	return g.bucket.OpenDownloadStream(id)
}

func (g *GridFS) Close() {

}

func (g *GridFS) OpenId(id bson.ObjectId) (file *GridFile,err error) {
	stream,err:=g.bucket.OpenDownloadStream(id)
	if err != nil {
		return
	}
	return &GridFile{
		gfs:      g,
		mode: gfsReading,
		stream:   stream,
	},nil
}
