package mgo

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/yaziming/mgo/bson"
	"sort"
	"testing"
)

func TestDatabase_CollectionNames(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		db1 := session.DB("db1")
		db1col1 := db1.C("col1")
		db1col2 := db1.C("col2")

		db2 := session.DB("db2")
		db2col1 := db2.C("col3")

		err = db1col1.Insert(M{"_id": 1})
		So(err, ShouldBeNil)
		err = db1col2.Insert(M{"_id": 1})
		So(err, ShouldBeNil)
		err = db2col1.Insert(M{"_id": 1})
		So(err, ShouldBeNil)

		names, err := session.DatabaseNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{"db1", "db2"})

		names, err = db1.CollectionNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{"col1", "col2"})

		names, err = db2.CollectionNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{"col3"})
	})
}

func TestDatabase_DropDatabase(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		db1 := session.DB("db1")
		_ = db1.C("col").Insert(M{"_id": 1})

		db2 := session.DB("db2")
		_ = db2.C("col").Insert(M{"_id": 1})

		err = db1.DropDatabase()
		So(err, ShouldBeNil)

		names, err := session.DatabaseNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{"db2"})

		err = db2.DropDatabase()
		So(err, ShouldBeNil)

		names, err = session.DatabaseNames()
		So(err, ShouldBeNil)
		So(filterDBs(names), ShouldResemble, []string{})
	})
}

func TestDatabase_CreateCollectionValidator(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		db := session.DB("mydb")
		coll := db.C("mycoll")

		// Test Validator.
		info := &CollectionInfo{
			Validator: M{"b": M{"$exists": true}},
		}
		err = coll.Create(info)
		So(err, ShouldBeNil)
		err = coll.Insert(M{"a": 1})
		So(err, ShouldErrorMatche, "Document failed validation")
		err = coll.DropCollection()
		So(err, ShouldBeNil)

		// Test ValidatorAction.
		info = &CollectionInfo{
			Validator:        M{"b": M{"$exists": true}},
			ValidationAction: "warn",
		}
		err = coll.Create(info)
		So(err, ShouldBeNil)
		err = coll.Insert(M{"a": 1})
		So(err, ShouldBeNil)
		err = coll.DropCollection()
		So(err, ShouldBeNil)

		// Test ValidationLevel.
		info = &CollectionInfo{
			Validator:       M{"a": M{"$exists": true}},
			ValidationLevel: "moderate",
		}
		err = coll.Create(info)
		So(err, ShouldBeNil)
		err = coll.Insert(M{"a": 1})
		So(err, ShouldBeNil)
		err = db.Run(bson.D{{Key: "collMod", Value: "mycoll"}, {Key: "validator", Value: M{"b": M{"$exists": true}}}}, nil)
		So(err, ShouldBeNil)
		err = coll.Insert(M{"a": 2})
		So(err, ShouldErrorMatche, "Document failed validation")
		err = coll.Update(M{"a": 1}, M{"c": 1})
		So(err, ShouldBeNil)
		err = coll.DropCollection()
		So(err, ShouldBeNil)
	})
}

func filterDBs(dbs []string) []string {
	var filtered []string
	for _, name := range dbs {
		switch name {
		case "admin", "local", "config", "system.indexes":
		default:
			filtered = append(filtered, name)
		}
	}
	if len(filtered) == 0 {
		return []string{}
	}
	sort.Strings(filtered)
	return filtered
}
