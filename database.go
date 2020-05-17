package mgo

import (
	"context"
	"github.com/Masterminds/semver"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo/gridfs"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Database session-driver db
type Database struct {
	database *mongo.Database
	version  *semver.Version
}

// C returns coll.
func (d *Database) C(collection string) *Collection {
	return &Collection{db: d, collection: d.database.Collection(collection)}
}

func (d *Database) GridFS(prefix string) *GridFS {
	opts := options.GridFSBucket().SetName(prefix)
	bucket, _ := gridfs.NewBucket(d.database, opts)
	return &GridFS{bucket: bucket, fileColl: &Collection{collection: bucket.GetFilesCollection()}}
}

func (d *Database) Run(bs interface{}, t interface{}) error {
	o := d.database.RunCommand(context.Background(), bs)
	if t == nil {
		return nil
	}
	return o.Decode(t)
}

func (d *Database) DropDatabase() error {
	return d.database.Drop(context.Background())
}

func (d *Database) Close() {
}

func (d *Database) CollectionNames() ([]string, error) {
	return d.database.ListCollectionNames(nil, bson.M{})
}
func (d *Database) Version() *semver.Version {
	return d.version
}
