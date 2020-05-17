package mgo

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestCollection_Index(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {

		session := ctx.mongo
		c := session.C("test_collection_index")
		indexes, err := c.Indexes()
		So(err, ShouldBeNil)
		So(indexes, ShouldHaveLength, 0)
		Convey("drop all indexes when coll not exits.", func() {
			err := c.DropAllIndexes()
			So(err, ShouldBeError)
			So(err.Error(), ShouldEqual, "(NamespaceNotFound) ns not found")
		})
		Convey("ensure index with key string", func() {
			err := c.EnsureIndexKey("name")
			So(err, ShouldBeNil)
			err = c.EnsureIndexKey("name2")
			So(err, ShouldBeNil)
			indexes, err := c.Indexes()
			So(err, ShouldBeNil)
			So(indexes, ShouldHaveLength, 3)
			Convey("drop all indexes", func() {
				err := c.DropAllIndexes()
				So(err, ShouldBeNil)
				indexes, err := c.Indexes()
				So(err, ShouldBeNil)
				So(indexes, ShouldHaveLength, 1)
				So(indexes[0].Key, ShouldHaveLength, 1)
				So(indexes[0].Key, ShouldContain, "_id")
			})
		})
		Convey("ensure index with mgo.Index", func() {
			err := c.EnsureIndex(Index{
				Key:           []string{"userID"},
				Unique:        true,
				Background:    false,
				Sparse:        false,
				PartialFilter: nil,
				ExpireAfter:   5 * time.Minute,
				Name:          "user_id",
			})
			Convey("assert ensure index", func() {
				So(err, ShouldBeNil)
				indexes, err := c.Indexes()
				So(err, ShouldBeNil)
				So(indexes, ShouldHaveLength, 2)
			})
			Convey("drop one index", func() {
				err := c.DropIndex("userID")
				So(err, ShouldBeNil)
				indexes, err := c.Indexes()
				So(err, ShouldBeNil)
				So(indexes, ShouldHaveLength, 1)
			})
		})

	})
}

func TestCollection_Bulk(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		bulk := coll.Bulk()
		bulk.Insert(M{"n": 1})
		bulk.Insert(M{"n": 2}, M{"n": 3})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r, ShouldHaveSameTypeAs, &BulkResult{})

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{1}, {2}, {3}})
	})
}

func TestCollection_DropCollection(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		db := session.DB("db1")
		_ = db.C("col1").Insert(M{"_id": 1})
		_ = db.C("col2").Insert(M{"_id": 1})

		err = db.C("col1").DropCollection()
		So(err, ShouldBeNil)

		names, err := db.CollectionNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{"col2"})

		err = db.C("col2").DropCollection()
		So(err, ShouldBeNil)

		names, err = db.CollectionNames()
		So(err, ShouldBeNil)
		So(len(filterDBs(names)), ShouldEqual, 0)
	})
}

func TestCollection_CreateCollectionCapped(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		info := &CollectionInfo{
			Capped:   true,
			MaxBytes: 1024,
			MaxDocs:  3,
		}
		err = coll.Create(info)
		So(err, ShouldBeNil)

		ns := []int{1, 2, 3, 4, 5}
		for _, n := range ns {
			err := coll.Insert(M{"n": n})
			So(err, ShouldBeNil)
		}

		n, err := coll.Find(nil).Count()
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 3)
	})
}
