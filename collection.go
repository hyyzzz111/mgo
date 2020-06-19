package mgo

import (
	"context"
	"fmt"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sort"
	"strings"
)

// Collection session-driver coll
type Collection struct {
	collection *mongo.Collection
	err        error
	db         *Database
}

func (c *Collection) DropCollection() error {
	return c.collection.Drop(nil)
}

// UpdateID updates a single document in the coll by id
func (c *Collection) FindId(id interface{}) *Query {
	return c.Find(bson.D{{Key: "_id", Value: queryID(id)}})
}

// Find finds docs by given filter
func (c *Collection) Find(filter interface{}) *Query {
	if filter == nil {
		filter = bson.D{}
	}
	return &Query{
		query: query{
			op: op{
				filter: filter,
			},
		},
		coll: c,
		err:  c.err,
	}
}

// Insert inserts a single document into the coll.
func (c *Collection) Insert(documents ...interface{}) (err error) {
	return c.InsertCtx(nil, documents...)
}
func (c *Collection) InsertCtx(ctx context.Context, documents ...interface{}) (err error) {
	_, err = c.InsertCtxWithResult(ctx, documents...)
	return err
}

// InsertAllWithResult inserts the provided documents and returns insert many result.
func (c *Collection) InsertCtxWithResult(ctx context.Context, documents ...interface{}) (result *mongo.InsertManyResult, err error) {
	result, err = c.collection.InsertMany(ctx, documents)
	return
}
func (c *Collection) Update(selector interface{}, update interface{}) (err error) {
	return c.UpdateOneCtx(nil, selector, update, false)
}
func (c *Collection) UpdateAll(selector interface{}, update interface{}) (info *ChangeInfo, err error) {
	return c.UpdateAllCtx(nil, selector, update)
}
func (c *Collection) UpdateAllCtx(ctx context.Context, selector interface{}, update interface{}) (info *ChangeInfo, err error) {
	result, err := c.UpdateAllCtxWithResult(ctx, selector, update, false)
	if err != nil {
		return
	}
	return &ChangeInfo{
		Updated:    int(result.ModifiedCount),
		Removed:    0,
		Matched:    int(result.MatchedCount),
		UpsertedId: result.UpsertedID,
	}, err
}

// UpdateID updates a single document in the coll by id
func (c *Collection) UpdateId(id interface{}, update interface{}) error {
	return c.Update(bson.M{"_id": queryID(id)}, update)
}
func (c *Collection) Upsert(selector interface{}, update interface{}) (info *ChangeInfo, err error) {
	r, err := c.UpdateOneCtxWithResult(context.Background(), selector, update, true)
	if err != nil {
		return
	}
	return &ChangeInfo{
		Updated:    int(r.ModifiedCount),
		Removed:    0,
		Matched:    int(r.MatchedCount),
		UpsertedId: r.UpsertedID,
	}, nil
}

// UpdateAll updates multiple documents in the coll.
func (c *Collection) UpdateAllCtxWithResult(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (*mongo.UpdateResult, error) {
	if selector == nil {
		selector = bson.D{}
	}
	var err error
	opt := options.Update()
	if len(upsert) > 0 {
		opt.SetUpsert(upsert[0])
	}

	var updateResult *mongo.UpdateResult
	if updateResult, err = c.collection.UpdateMany(ctx, selector, update, opt); err != nil {
		return updateResult, err
	}
	return updateResult, nil
}
func (c *Collection) ReplaceOneCtx(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (err error) {
	_, err = c.ReplaceOneCtxWithResult(ctx, selector, update, upsert...)
	return err
}
func (c *Collection) ReplaceOneCtxWithResult(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (result *mongo.UpdateResult, err error) {

	if selector == nil {
		selector = bson.D{}
	}
	opt := options.Replace()
	if len(upsert) > 0 {
		opt.SetUpsert(upsert[0])
	}
	result, err = c.collection.ReplaceOne(ctx, selector, update, opt)
	if err != nil {
		return
	}
	if result.UpsertedCount == 0 && result.MatchedCount == 0 {
		return result, ErrNotFound
	}
	return
}

// Update updates a single document in the coll.
func (c *Collection) UpdateOneCtx(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (err error) {
	_, err = c.UpdateOneCtxWithResult(ctx, selector, update, upsert...)

	return err

}

// UpdateOneCtxWithResult updates a single document in the coll and returns update result.
func (c *Collection) UpdateOneCtxWithResult(ctx context.Context, selector interface{}, update interface{}, upsert ...bool) (result *mongo.UpdateResult, err error) {
	result, err = c.ReplaceOneCtxWithResult(ctx, selector, update, upsert...)
	if err == nil || err.Error() != "replacement document cannot contains keys beginning with '$" {
		return
	}
	if selector == nil {
		selector = bson.D{}
	}

	opt := options.Update()
	if len(upsert) > 0 {
		opt.SetUpsert(upsert[0])
	}
	result, err = c.collection.UpdateOne(ctx, selector, update, opt)
	if err != nil {
		return
	}
	if result.UpsertedCount == 0 && result.MatchedCount == 0 {
		return result, ErrNotFound
	}
	return result, err
}

func (c *Collection) RemoveId(id interface{}) error {
	return c.Remove(bson.D{{Key: "_id", Value: queryID(id)}})
}

// Remove deletes a single document from the coll.
func (c *Collection) Remove(selector interface{}) (err error) {

	if selector == nil {
		selector = bson.D{}
	}
	if _, err = c.collection.DeleteOne(nil, selector); err != nil {
		return err
	}
	return nil
}

// RemoveAll deletes multiple documents from the coll.
func (c *Collection) RemoveAll(selector interface{}) (info *ChangeInfo, err error) {

	if selector == nil {
		selector = bson.D{}
	}
	mResult, err := c.collection.DeleteMany(nil, selector)
	if err != nil {
		return
	}
	return &ChangeInfo{
		Removed: int(mResult.DeletedCount),
	}, nil
}
func (c *Collection) Count() (count int, err error) {
	return c.CountBy(nil)
}

// Count gets the number of documents matching the filter.
func (c *Collection) CountBy(selector interface{}) (count int, err error) {

	if selector == nil {
		selector = bson.D{}
	}
	var count64 int64
	count64, err = c.collection.CountDocuments(context.TODO(), selector)
	return int(count64), err
}
func (c *Collection) Pipe(pipeline interface{}) *Pipe {
	return &Pipe{pipeline: pipeline, coll: c}
}

func (c *Collection) EnsureIndex(index Index) (err error) {

	models, err := index.ToIndexModels()
	_, err = c.collection.Indexes().CreateOne(context.TODO(), models)
	return err
}
func (c *Collection) DropAllIndexes() (err error) {
	_, err = c.collection.Indexes().DropAll(nil)
	return
}
func (c *Collection) DropIndex(key ...string) (err error) {
	indexes, err := c.Indexes()
	if err != nil {
		return err
	}
	sort.Strings(key)
	name := ""
	for _, index := range indexes {
		sort.Strings(index.Key)

		if strings.Join(index.Key, "_") == strings.Join(key, "_") {
			name = index.Name
			break
		}
	}
	if name == "" {
		return ErrIndexNotFound
	}

	return c.DropIndexName(name)
}
func (c *Collection) DropIndexName(name string) error {
	_, err := c.collection.Indexes().DropOne(nil, name)
	return err
}
func (c *Collection) EnsureIndexKey(key ...string) (err error) {
	return c.EnsureIndex(Index{
		Key: key,
	})
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

func (c *Collection) Bulk() *Bulk {
	return &Bulk{c: c, ordered: true}
}

func (c *Collection) UpsertId(i interface{}, doc bson.M) (*ChangeInfo, error) {
	return c.Upsert(bson.M{"_id": queryID(i)}, doc)
}

func (c *Collection) Database() *Database {
	return c.db
}
