package mgo

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestBulk_InsertError(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		bulk := coll.Bulk()
		bulk.Insert(M{"_id": 1}, M{"_id": 2}, M{"_id": 2}, M{"_id": 3})
		_, err = bulk.Run()
		So(err, ShouldBeError, "{E11000 duplicate key error collection: mydb.mycoll index: _id_ dup key: { _id: 2 }}")
		So(IsDup(err), ShouldBeTrue)

		type doc struct {
			N int `bson:"_id"`
		}
		var res []doc
		err = coll.Find(nil).Sort("_id").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{1}, {2}})
	})
}

func TestBulk_InsertErrorUnordered(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		bulk := coll.Bulk()
		bulk.Unordered()
		bulk.Insert(M{"_id": 1}, M{"_id": 2}, M{"_id": 2}, M{"_id": 3})
		_, err = bulk.Run()
		So(err, ShouldBeError, "{E11000 duplicate key error collection: mydb.mycoll index: _id_ dup key: { _id: 2 }}")

		type doc struct {
			N int `bson:"_id"`
		}
		var res []doc
		err = coll.Find(nil).Sort("_id").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{1}, {2}, {3}})
	})
}

func TestBulk_InsertErrorUnorderedSplitBatch(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		bulk := coll.Bulk()
		bulk.Unordered()
		const total = 4096
		type doc struct {
			Id int `bson:"_id"`
		}
		docs := make([]interface{}, total)
		for i := 0; i < total; i++ {
			docs[i] = doc{i}
		}
		docs[1] = doc{0}
		bulk.Insert(docs...)
		_, err = bulk.Run()
		So(err, ShouldBeError, "{E11000 duplicate key error collection: mydb.mycoll index: _id_ dup key: { _id: 0 }}")
		n, err := coll.Count()
		So(err, ShouldBeNil)
		So(n, ShouldEqual, total-1)

		var res doc
		err = coll.FindId(1500).One(&res)
		So(err, ShouldBeNil)
		So(res.Id, ShouldEqual, 1500)
	})
}
func TestBulk_InsertErrorString(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		// If it's just the same string multiple times, join it into a single message.
		bulk := coll.Bulk()
		bulk.Unordered()
		bulk.Insert(M{"_id": 1}, M{"_id": 2}, M{"_id": 2})

		_, err = bulk.Run()
		So(err, ShouldHaveSameTypeAs, &BulkError{})
		So(IsDup(err), ShouldEqual, true)

		// With matching errors but different messages, present them all.
		bulk = coll.Bulk()
		bulk.Unordered()
		bulk.Insert(M{"_id": "dupone"}, M{"_id": "dupone"}, M{"_id": "duptwo"}, M{"_id": "duptwo"})
		_, err = bulk.Run()
		So(err, ShouldHaveSameTypeAs, &BulkError{})
		So(IsDup(err), ShouldEqual, true)

		// With mixed errors, present them all.
		bulk = coll.Bulk()
		bulk.Unordered()
		bulk.Insert(M{"_id": 1}, M{"_id": []int{2}})
		_, err = bulk.Run()
		So(IsDup(err), ShouldEqual, false)

	})
}
func Test_BulkErrorCases(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		bulk := coll.Bulk()

		// There's a limit of 1000 operations per command, so
		// this forces the more complex indexing logic to act.
		for i := 0; i < 20; i++ {
			switch i {
			case 3, 14:
				bulk.Insert(M{"_id": "dupone"})
			case 7, 17:
				bulk.Insert(M{"_id": "duptwo"})
			default:
				bulk.Insert(M{"_id": i})
			}
		}
		_, err = bulk.Run()
		So(err, ShouldNotHaveSameTypeAs, BulkError{})

	})
}
func TestBulk_Update(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3})
		So(err, ShouldBeNil)

		bulk := coll.Bulk()
		bulk.Update(M{"n": 1}, M{"$set": M{"n": 1}})
		bulk.Update(M{"n": 2}, M{"$set": M{"n": 20}})
		bulk.Update(M{"n": 5}, M{"$set": M{"n": 50}}) // Won't match.
		bulk.Update(M{"n": 1}, M{"$set": M{"n": 10}}, M{"n": 3}, M{"$set": M{"n": 30}})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r.Matched, ShouldEqual, 4)
		So(r.Modified, ShouldEqual, 3)

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{10}, {20}, {30}})

	})
}
func TestBulk_UpdateError(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3})
		So(err, ShouldBeNil)
		bulk := coll.Bulk()
		bulk.Update(
			M{"n": 1}, M{"$set": M{"n": 10}},
			M{"n": 2}, M{"$set": M{"n": 20, "_id": 20}},
			M{"n": 3}, M{"$set": M{"n": 30}},
		)
		r, err := bulk.Run()
		So(err, ShouldBeError, "{Performing an update on the path '_id' would modify the immutable field '_id'}")
		So(r, ShouldHaveSameTypeAs, &BulkResult{})

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{2}, {3}, {10}})
	})
}
func TestBulk_UpdateAll(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3})
		So(err, ShouldBeNil)

		bulk := coll.Bulk()
		bulk.UpdateAll(M{"n": 1}, M{"$set": M{"n": 10}})
		bulk.UpdateAll(M{"n": 2}, M{"$set": M{"n": 2}})  // Won't change.
		bulk.UpdateAll(M{"n": 5}, M{"$set": M{"n": 50}}) // Won't match.
		bulk.UpdateAll(M{}, M{"$inc": M{"n": 1}}, M{"n": 11}, M{"$set": M{"n": 5}})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r.Matched, ShouldEqual, 6)
		So(r.Modified, ShouldEqual, 5)

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{3}, {4}, {5}})
	})
}

func TestBulk_MixedUnordered(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		// Abuse undefined behavior to ensure the desired implementation is in place.
		bulk := coll.Bulk()
		bulk.Unordered()
		bulk.Insert(M{"n": 1})
		bulk.Update(M{"n": 2}, M{"$inc": M{"n": 1}})
		bulk.Insert(M{"n": 2})
		bulk.Update(M{"n": 3}, M{"$inc": M{"n": 1}})
		bulk.Update(M{"n": 1}, M{"$inc": M{"n": 1}})
		bulk.Insert(M{"n": 3})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r.Matched, ShouldEqual, 3)
		So(r.Modified, ShouldEqual, 3)

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{2}, {3}, {4}})
	})
}

func TestBulk_Upsert(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3})
		So(err, ShouldBeNil)

		bulk := coll.Bulk()
		bulk.Upsert(M{"n": 2}, M{"$set": M{"n": 20}})
		bulk.Upsert(M{"n": 4}, M{"$set": M{"n": 40}}, M{"n": 3}, M{"$set": M{"n": 30}})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r, ShouldHaveSameTypeAs, &BulkResult{})

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{1}, {20}, {30}, {40}})
	})
}
func TestBulk_Remove(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3}, M{"n": 4}, M{"n": 4})
		So(err, ShouldBeNil)

		bulk := coll.Bulk()
		bulk.Remove(M{"n": 1})
		bulk.Remove(M{"n": 2}, M{"n": 4})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r.Matched, ShouldEqual, 0)
		So(r.Deleted, ShouldEqual, 3)

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{3}, {4}})
	})
}

func TestBulk_RemoveAll(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1}, M{"n": 2}, M{"n": 3}, M{"n": 4}, M{"n": 4})
		So(err, ShouldBeNil)

		bulk := coll.Bulk()
		bulk.RemoveAll(M{"n": 1})
		bulk.RemoveAll(M{"n": 2}, M{"n": 4})
		r, err := bulk.Run()
		So(err, ShouldBeNil)
		So(r.Matched, ShouldEqual, 0)
		So(r.Deleted, ShouldEqual, 4)

		type doc struct{ N int }
		var res []doc
		err = coll.Find(nil).Sort("n").All(&res)
		So(err, ShouldBeNil)
		So(res, ShouldResemble, []doc{{3}})
	})
}
