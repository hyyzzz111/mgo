package mgo

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

type M = bson.M

func TestPipe_Iter(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			_ = coll.Insert(M{"n": n})
		}
		pipe := coll.Pipe([]M{{"$match": M{"n": M{"$gte": 42}}}})
		pipe.Batch(2)
		pipe.AllowDiskUse()
		iter := pipe.Iter()
		result := struct{ N int }{}
		for i := 2; i < 7; i++ {
			ok := iter.Next(&result)
			So(ok, ShouldBeTrue)
			So(result.N, ShouldEqual, ns[i])
		}
		So(iter.Next(&result), ShouldBeFalse)
		So(iter.Close(), ShouldBeNil)
	})
}
func TestPipe_All(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"n": n})
			So(err, ShouldBeNil)
		}

		var result []struct{ N int }
		err = coll.Pipe([]M{{"$match": M{"n": M{"$gte": 42}}}}).All(&result)
		So(err, ShouldBeNil)
		for i := 2; i < 7; i++ {
			So(result[i-2].N, ShouldEqual, ns[i])
		}
	})
}
func TestPipe_One(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo

		coll := session.DB("mydb").C("mycoll")
		_ = coll.Insert(M{"a": 1, "b": 2})

		result := struct{ A, B int }{}

		pipe := coll.Pipe([]M{{"$project": M{"a": 1, "b": M{"$add": []interface{}{"$b", 1}}}}})
		err = pipe.One(&result)
		So(err, ShouldBeNil)
		So(result.A, ShouldEqual, 1)
		So(result.B, ShouldEqual, 3)

		pipe = coll.Pipe([]M{{"$match": M{"a": 2}}})
		err = pipe.One(&result)
		So(err, ShouldEqual, ErrNotFound)
	})
}
func TestPipe_Explain(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		_ = coll.Insert(M{"a": 1, "b": 2})

		pipe := coll.Pipe([]M{{"$project": M{"a": 1, "b": M{"$add": []interface{}{"$b", 1}}}}})

		// The explain command result changes across versions.
		var result struct{ Ok int }
		err = pipe.Explain(&result)
		So(err, ShouldBeNil)
		So(result.Ok, ShouldEqual, 1)
	})
}
func TestPipe_Collation(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		beatles := []string{"John", "RINGO", "George", "Paul"}
		for _, n := range beatles {
			err = coll.Insert(M{"name": n})
			So(err, ShouldBeNil)
		}

		collation := &Collation{
			Locale:   "en",
			Strength: 1, // ignore case/diacritics
		}
		var result []struct{ Name string }
		err = coll.Pipe([]M{{"$match": M{"name": "ringo"}}}).Collation(collation).All(&result)
		So(err, ShouldBeNil)
		So(result, ShouldHaveLength, 1)
		So(result[0].Name, ShouldEqual, "RINGO")
	})
}
func TestPipe_IterDoneErr(t *testing.T) {
	AuthMongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		iter := coll.Find(nil).Iter()

		result := struct{}{}
		ok := iter.Next(&result)
		So(iter.Done(), ShouldBeTrue)
		So(ok, ShouldBeFalse)
		So(iter.Err(), ShouldBeError, mongo.CommandError{Name: "Unauthorized", Message: "command find requires authentication"})
	})
}
func TestPipe_IterDoneNotFound(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		result := struct{ A, B int }{}
		iter := coll.Find(M{"a": 1}).Iter()
		ok := iter.Next(&result)
		So(iter.Done(), ShouldBeTrue)
		So(ok, ShouldBeFalse)
	})
}
