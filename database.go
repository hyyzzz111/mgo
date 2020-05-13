package mgo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo/gridfs"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Database session-driver database
type Database struct {
	database *mongo.Database
}

// CollectionNames returns the collection names present in database.
func (d *Database) CollectionNames() (names []string, err error) {
	names, err = d.database.ListCollectionNames(context.TODO(), options.ListCollectionsOptions{})
	return
}

// C returns collection.
func (d *Database) C(collection string) *Collection {
	return &Collection{collection: d.database.Collection(collection)}
}

// Collection returns collection.
func (d *Database) Collection(collection string) *Collection {
	return &Collection{collection: d.database.Collection(collection)}
}

func (d *Database) GridFS(prefix string) *GridFS {
	opts := options.GridFSBucket().SetName(prefix)
	bucket, _ := gridfs.NewBucket(d.database, opts)
	return &GridFS{bucket: bucket, fileColl: &Collection{collection: bucket.GetFilesCollection()}}
}

func (d *Database) Run(bs interface{}, t interface{}) error {
	o:=d.database.RunCommand(context.Background(),bs)
	return o.Decode(&t)
}

func (d *Database) DropDatabase() error {
	return d.database.Drop(context.Background())
}

func (d *Database) Close() {
}
