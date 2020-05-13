package mgo

import (
	"context"
	"fmt"
	"github.com/yaziming/mgo/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Collection session-driver collection
type Collection struct {
	collection *mongo.Collection
}

func (c *Collection) Drop() error {
	return c.collection.Drop(context.TODO())
}

// UpdateID updates a single document in the collection by id
func (c *Collection) FindId(id interface{}) *Query {
	var bsonID bson.ObjectId
	switch newId := id.(type) {
	case string:
		bsonID = bson.ObjectIdHex(newId)
	case bson.ObjectId:
		bsonID = newId
	default:
	}
	return c.Find(bson.D{{Key: "_id", Value: bsonID}})
}

// Find finds docs by given filter
func (c *Collection) Find(filter interface{}) *Query {
	if filter == nil {
		filter = bson.D{}
	}
	return &Query{op: op{
		filter: filter,
	}, collection: c.collection}
}

// Insert inserts a single document into the collection.
func (c *Collection) Insert(documents ...interface{}) (err error) {
	_, err = c.InsertWithResult(documents...)
	return err
}
func (c *Collection) InsertCtx(ctx context.Context, documents ...interface{}) (err error) {
	_, err = c.InsertCtxWithResult(ctx, documents...)
	return err
}

// InsertWithResult inserts a single document into the collection and returns insert one result.
func (c *Collection) InsertWithResult(documents ...interface{}) (result *mongo.InsertManyResult, err error) {
	return c.InsertCtxWithResult(context.TODO(), documents...)
}

// InsertAllWithResult inserts the provided documents and returns insert many result.
func (c *Collection) InsertCtxWithResult(ctx context.Context, documents ...interface{}) (result *mongo.InsertManyResult, err error) {
	result, err = c.collection.InsertMany(ctx, documents)
	return
}
func (c *Collection) Update(selector interface{}, update interface{}, upsert ...bool) (err error) {
	return c.UpdateOneCtx(context.TODO(), selector, update, upsert...)
}

func (c *Collection) ReplaceOneCtx(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (err error) {
	if selector == nil {
		selector = bson.D{}
	}
	opt := options.Replace()
	if len(upsert) > 0 {
		opt.SetUpsert(upsert[0])
	}

	_, err = c.collection.ReplaceOne(ctx, selector, update, opt)
	return err
}

// Update updates a single document in the collection.
func (c *Collection) UpdateOneCtx(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (err error) {
	err = c.ReplaceOneCtx(ctx, selector, update, upsert...)
	if err == nil || err.Error() != "replacement document cannot contains keys beginning with '$" {
		return err
	}
	if selector == nil {
		selector = bson.D{}
	}

	opt := options.Update()
	if len(upsert) > 0 {
		opt.SetUpsert(upsert[0])
	}
	_, err = c.collection.UpdateOne(ctx, selector, update, opt)
	return err

}

// UpdateWithResult updates a single document in the collection and returns update result.
func (c *Collection) UpdateWithResult(selector interface{}, update interface{}, upsert ...bool) (result *mongo.UpdateResult, err error) {
	if selector == nil {
		selector = bson.D{}
	}

	opt := options.Update()
	for _, arg := range upsert {
		if arg {
			opt.SetUpsert(arg)
		}
	}

	result, err = c.collection.UpdateOne(context.TODO(), selector, update, opt)
	return
}

// UpdateID updates a single document in the collection by id
func (c *Collection) UpdateId(id interface{}, update interface{}) error {
	var bsonID bson.ObjectId
	switch newId := id.(type) {
	case string:
		bsonID = bson.ObjectIdHex(newId)
	case bson.ObjectId:
		bsonID = newId
	default:
	}
	return c.Update(bson.M{"_id": bsonID}, update)
}

// UpdateAll updates multiple documents in the collection.
func (c *Collection) UpdateAll(selector interface{}, update interface{}, upsert ...bool) (*mongo.UpdateResult, error) {
	if selector == nil {
		selector = bson.D{}
	}

	var err error

	opt := options.Update()
	for _, arg := range upsert {
		if arg {
			opt.SetUpsert(arg)
		}
	}

	var updateResult *mongo.UpdateResult
	if updateResult, err = c.collection.UpdateMany(context.TODO(), selector, update, opt); err != nil {
		return updateResult, err
	}
	return updateResult, nil
}

func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{Key: "_id", Value: id}})
}

// Remove deletes a single document from the collection.
func (c *Collection) Remove(selector interface{}) error {
	if selector == nil {
		selector = bson.D{}
	}
	var err error
	if _, err = c.collection.DeleteOne(context.TODO(), selector); err != nil {
		return err
	}
	return nil
}

// RemoveID deletes a single document from the collection by id.
func (c *Collection) RemoveID(id interface{}) error {
	var bsonID bson.ObjectId
	switch newId := id.(type) {
	case string:
		bsonID = bson.ObjectIdHex(newId)
	case bson.ObjectId:
		bsonID = newId
	default:
	}
	return c.Remove(bson.M{"_id": bsonID})
}

// RemoveAll deletes multiple documents from the collection.
func (c *Collection) RemoveAll(selector interface{}) (info *ChangeInfo, err error) {
	if selector == nil {
		selector = bson.D{}
	}
	mResult, err := c.collection.DeleteMany(context.TODO(), selector)
	if err != nil {
		return
	}
	return &ChangeInfo{
		Removed: int(mResult.DeletedCount),
	}, nil
}

// Count gets the number of documents matching the filter.
func (c *Collection) Count(selector interface{}) (int, error) {
	if selector == nil {
		selector = bson.D{}
	}
	var err error
	var count int64
	count, err = c.collection.CountDocuments(context.TODO(), selector)
	return int(count), err
}
func (s *Collection) Pipe(pipeline interface{}) *Pipe {
	return &Pipe{pipeline: pipeline, collection: s.collection}
}

// Limit specifies a limit on the number of results.
// A negative limit implies that only 1 batch should be returned.
func (s *Session) Limit(limit int) *Session {
	tmp := int64(limit)

	s.limit = &tmp
	return s
}

// Skip specifies the number of documents to skip before returning.
// For server versions < 3.2, this defaults to 0.
func (s *Session) Skip(skip int) *Session {
	tmp := int64(skip)
	s.skip = &tmp
	return s
}

// Sort specifies the order in which to return documents.
func (s *Session) Sort(sort interface{}) *Session {
	s.sort = sort
	return s
}

func (s *Collection) EnsureIndex(index Index) (err error) {

	_, err = s.collection.Indexes().CreateOne(context.TODO(), index.ToIndexModels())
	return err
}

func (c *Collection) DropIndex(field string) (err error) {
	return nil
}

func (c *Collection) EnsureIndexKey(s string) (err error) {

	return nil
}

func (c *Collection) DropCollection() error {
	return c.collection.Drop(context.Background())
}

type CollectionInfo struct {
	// DisableIdIndex prevents the automatic creation of the index
	// on the _id field for the collection.

	// ForceIdIndex enforces the automatic creation of the index
	// on the _id field for the collection. Capped collections,
	// for example, do not have such an index by default.
	ForceIdIndex bool

	// If Capped is true new documents will replace old ones when
	// the collection is full. MaxBytes must necessarily be set
	// to define the size when the collection wraps around.
	// MaxDocs optionally defines the number of documents when it
	// wraps, but MaxBytes still needs to be set.
	Capped   bool
	MaxBytes int
	MaxDocs  int

	// Validator contains a validation expression that defines which
	// documents should be considered valid for this collection.
	Validator interface{}

	// ValidationLevel may be set to "strict" (the default) to force
	// MongoDB to validate all documents on inserts and updates, to
	// "moderate" to apply the validation rules only to documents
	// that already fulfill the validation criteria, or to "off" for
	// disabling validation entirely.
	ValidationLevel string

	// ValidationAction determines how MongoDB handles documents that
	// violate the validation rules. It may be set to "error" (the default)
	// to reject inserts or updates that violate the rules, or to "warn"
	// to log invalid operations but allow them to proceed.
	ValidationAction string

	// StorageEngine allows specifying collection options for the
	// storage engine in use. The map keys must hold the storage engine
	// name for which options are being specified.
	StorageEngine interface{}
	// Specifies the default collation for the collection.
	// Collation allows users to specify language-specific rules for string
	// comparison, such as rules for lettercase and accent marks.
	Collation *Collation
}

func (c *Collection) Create(info *CollectionInfo) error {
	opts := options.CreateCollection()

	if info.Capped {
		if info.MaxBytes < 1 {
			return fmt.Errorf("Collection.Create: with Capped, MaxBytes must also be set")
		}
		opts.SetCapped(true).SetSizeInBytes(int64(info.MaxBytes))
		if info.MaxDocs > 0 {
			opts.SetMaxDocuments(int64(info.MaxDocs))
		}
	}

	if info.ForceIdIndex {
	}
	if info.Validator != nil {
		opts.SetValidator(info.Validator)
	}
	if info.ValidationLevel != "" {
		opts.SetValidationLevel(info.ValidationLevel)
	}
	if info.ValidationAction != "" {
		opts.SetValidationAction(info.ValidationAction)
	}
	if info.StorageEngine != nil {
		opts.SetStorageEngine(info.StorageEngine)
	}
	if info.Collation != nil {
		opts.SetCollation(info.Collation)
	}
	return c.collection.Database().CreateCollection(context.Background(), c.collection.Name(), opts)
}

func (c *Collection) Indexes() (indexes []Index, err error) {
	cursor, err := c.collection.Indexes().List(context.Background())
	if err != nil {
		return
	}
	err = cursor.Decode(&indexes)
	return
}
