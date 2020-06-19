package mgo

import (
	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
	"mgo/bson"
	"sort"
	"strings"
	"testing"
)

func TestQuery_Select(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		_ = coll.Insert(M{"a": 1, "b": 2})

		result := struct{ A, B int }{}
		err = coll.Find(M{"a": 1}).Select(M{"b": 1}).One(&result)
		So(err, ShouldBeNil)
		So(result.A, ShouldEqual, 0)
		So(result.B, ShouldEqual, 2)
		err = coll.Find(M{"a": 1}).Skip(1).One(nil)
		So(err, ShouldBeError, ErrNotFound)
	})
}
func TestQuery_RawDocument(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")
		err = coll.Insert(bson.M{"key": "w", "value": bson.M{"a": 1, "b": 2}})
		var setting struct {
			Key   string
			Value bson.Raw
		}
		var R struct {
			A int
			B int
		}
		err = coll.Find(bson.M{"key": "w"}).One(&setting)
		So(err, ShouldBeNil)
		So(setting.Value, ShouldNotBeNil)
		err = setting.Value.Unmarshal(&R)
		So(err, ShouldBeNil)
		So(R.A, ShouldEqual, 1)
		So(R.B, ShouldEqual, 2)
		b, err := bson.Marshal(R)
		So(err, ShouldBeNil)
		err = bson.Unmarshal(b, &R)
		So(err, ShouldBeNil)
		So(R.A, ShouldEqual, 1)
		So(R.B, ShouldEqual, 2)
	})
}
func TestQuery_InlineMap(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		var v, result1 struct {
			A int
			M map[string]int `bson:",inline"`
		}

		v.A = 1
		v.M = map[string]int{"b": 2}
		err = coll.Insert(v)
		So(err, ShouldBeNil)

		noId := M{"_id": 0}

		err = coll.Find(nil).Select(noId).One(&result1)
		So(err, ShouldBeNil)
		So(result1.A, ShouldEqual, 1)
		So(result1.M, ShouldResemble, map[string]int{"b": 2})

		var result2 bson.M
		err = coll.Find(nil).Select(noId).One(&result2)
		So(err, ShouldBeNil)
		So(result2, ShouldNotResemble, bson.M{"a": 1, "b": 2})
		So(result2, ShouldResemble, bson.M{"a": int32(1), "b": int32(2)})
	})
}

func TestQuery_Update(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"k": n, "n": n})
			So(err, ShouldBeNil)
		}

		// No changes is a no-op and shouldn't return an error.
		err = coll.Update(M{"k": 42}, M{"$set": M{"n": 42}})
		So(err, ShouldBeNil)

		err = coll.Update(M{"k": 42}, M{"$inc": M{"n": 1}})
		So(err, ShouldBeNil)

		result := make(M)
		err = coll.Find(M{"k": 42}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 43)

		err = coll.Update(M{"k": 47}, M{"k": 47, "n": 47})
		So(err, ShouldBeError, ErrNotFound)

		err = coll.Find(M{"k": 47}).One(result)
		So(err, ShouldBeError, ErrNotFound)
	})
}

func TestQuery_UpdateId(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"_id": n, "n": n})
			So(err, ShouldBeNil)
		}

		err = coll.UpdateId(42, M{"$inc": M{"n": 1}})
		So(err, ShouldBeNil)

		result := make(M)
		err = coll.FindId(42).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 43)

		err = coll.UpdateId(47, M{"k": 47, "n": 47})
		So(err, ShouldBeError, ErrNotFound)

		err = coll.FindId(47).One(result)
		So(err, ShouldBeError, ErrNotFound)
	})
}

func TestQuery_UpdateNil(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"k": 42, "n": 42})
		So(err, ShouldBeNil)
		err = coll.Update(nil, M{"$inc": M{"n": 1}})
		So(err, ShouldBeNil)

		result := make(M)
		err = coll.Find(M{"k": 42}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 43)

		err = coll.Insert(M{"k": 45, "n": 45})
		So(err, ShouldBeNil)
		_, err = coll.UpdateAll(nil, M{"$inc": M{"n": 1}})
		So(err, ShouldBeNil)

		err = coll.Find(M{"k": 42}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 44)
		err = coll.Find(M{"k": 45}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 46)
	})
}

func TestQuery_Upsert(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(bson.D{{Key: "k", Value: n}, {Key: "n", Value: n}})
			So(err, ShouldBeNil)
		}

		info, err := coll.Upsert(M{"k": 42}, bson.D{{Key: "k", Value: 42}, {Key: "n", Value: 24}})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 1)
		So(info.Matched, ShouldEqual, 1)
		So(info.UpsertedId, ShouldBeNil)

		result := M{}
		err = coll.Find(M{"k": 42}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 24)

		// Match but do not change.
		info, err = coll.Upsert(M{"k": 42}, bson.D{{Key: "k", Value: 42}, {Key: "n", Value: 24}})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 1) // On 2.6+ this feels like a server mistake.
		So(info.Matched, ShouldEqual, 1)
		So(info.UpsertedId, ShouldBeNil)

		// Insert with internally created id.
		info, err = coll.Upsert(M{"k": 47}, M{"k": 47, "n": 47})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.Matched, ShouldEqual, 0)
		So(info.UpsertedId, ShouldNotBeNil)

		err = coll.Find(M{"k": 47}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 47)

		result = M{}
		err = coll.Find(M{"_id": info.UpsertedId}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 47)

		// Insert with provided id.
		info, err = coll.Upsert(M{"k": 48}, M{"k": 48, "n": 48, "_id": 48})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.Matched, ShouldEqual, 0)
		So(info.UpsertedId, ShouldEqual, 48)

		err = coll.Find(M{"k": 48}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 48)
	})
}

func TestQuery_UpsertId(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"_id": n, "n": n})
			So(err, ShouldBeNil)
		}

		info, err := coll.UpsertId(42, M{"n": 24})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 1)
		So(info.UpsertedId, ShouldBeNil)

		result := M{}
		err = coll.FindId(42).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 24)

		info, err = coll.UpsertId(47, M{"_id": 47, "n": 47})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.UpsertedId, ShouldEqual, 47)

		err = coll.FindId(47).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 47)
	})
}

func TestQuery_UpdateAll(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"k": n, "n": n})
			So(err, ShouldBeNil)
		}

		info, err := coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$unset": M{"missing": 1}})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.Matched, ShouldEqual, 4)

		info, err = coll.UpdateAll(M{"k": M{"$gt": 42}}, M{"$inc": M{"n": 1}})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 4)
		So(info.Matched, ShouldEqual, 4)

		result := make(M)
		err = coll.Find(M{"k": 42}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 42)

		err = coll.Find(M{"k": 43}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 44)

		err = coll.Find(M{"k": 44}).One(result)
		So(err, ShouldBeNil)
		So(result["n"], ShouldEqual, 45)
	})
}

func TestQuery_Remove(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"n": n})
			So(err, ShouldBeNil)
		}

		err = coll.Remove(M{"n": M{"$gt": 42}})
		So(err, ShouldBeNil)

		result := &struct{ N int }{}
		err = coll.Find(M{"n": 42}).One(result)
		So(err, ShouldBeNil)
		So(result.N, ShouldEqual, 42)

		err = coll.Find(M{"n": 43}).One(result)
		So(err, ShouldEqual, ErrNotFound)

		err = coll.Find(M{"n": 44}).One(result)
		So(err, ShouldBeNil)
		So(result.N, ShouldEqual, 44)
	})
}
func TestQuery_RemoveId(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"_id": 40}, M{"_id": 41}, M{"_id": 42})
		So(err, ShouldBeNil)

		err = coll.RemoveId(41)
		So(err, ShouldBeNil)

		So(coll.FindId(40).One(nil), ShouldBeNil)
		So(coll.FindId(41).One(nil), ShouldEqual, ErrNotFound)
		So(coll.FindId(42).One(nil), ShouldBeNil)

	})
}

func TestQuery_RemoveAll(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42, 43, 44, 45, 46}
		for _, n := range ns {
			err := coll.Insert(M{"n": n})
			So(err, ShouldBeNil)
		}

		info, err := coll.RemoveAll(M{"n": M{"$gt": 42}})
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.Removed, ShouldEqual, 4)
		So(info.Matched, ShouldEqual, 0)
		So(info.UpsertedId, ShouldBeNil)
		//
		result := &struct{ N int }{}
		err = coll.Find(M{"n": 42}).One(result)
		So(err, ShouldBeNil)
		So(result.N, ShouldEqual, 42)
		err = coll.Find(M{"n": 43}).One(result)
		spew.Dump(err, 2)
		So(err, ShouldEqual, ErrNotFound)

		err = coll.Find(M{"n": 44}).One(result)
		So(err, ShouldEqual, ErrNotFound)

		info, err = coll.RemoveAll(nil)
		So(err, ShouldBeNil)
		So(info.Updated, ShouldEqual, 0)
		So(info.Removed, ShouldEqual, 3)
		So(info.Matched, ShouldEqual, 0)
		So(info.UpsertedId, ShouldBeNil)

		n, err := coll.Find(nil).Count()
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 0)

	})
}

func TestQuery_Distinct(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		for _, i := range []int{1, 4, 6, 2, 2, 3, 4} {
			_ = coll.Insert(M{"n": i})
		}

		var result []int
		err = coll.Find(M{"n": M{"$gt": 2}}).Sort("n").Distinct("n", &result)
		So(err, ShouldBeNil)
		sort.IntSlice(result).Sort()
		So(result, ShouldResemble, []int{3, 4, 6})
	})
}

func TestQuery_Sort(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		_ = coll.Insert(M{"a": 1, "b": 1})
		_ = coll.Insert(M{"a": 2, "b": 2})
		_ = coll.Insert(M{"a": 2, "b": 1})
		_ = coll.Insert(M{"a": 0, "b": 1})
		_ = coll.Insert(M{"a": 2, "b": 0})
		_ = coll.Insert(M{"a": 0, "b": 2})
		_ = coll.Insert(M{"a": 1, "b": 2})
		_ = coll.Insert(M{"a": 0, "b": 0})
		_ = coll.Insert(M{"a": 1, "b": 0})

		query := coll.Find(M{})
		query.Sort("-a") // Should be ignored.
		query.Sort("-b", "a")
		iter := query.Iter()

		l := make([]int, 18)
		r := struct{ A, B int }{}
		for i := 0; i != len(l); i += 2 {
			ok := iter.Next(&r)
			So(ok, ShouldEqual, true)
			l[i] = r.A
			l[i+1] = r.B
		}

		So(l, ShouldResemble, []int{0, 2, 1, 2, 2, 2, 0, 1, 1, 1, 2, 1, 0, 0, 1, 0, 2, 0})
	})
}

func TestQuery_Sort2(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		f1 := func() { coll.Find(nil).Sort("") }
		f2 := func() { coll.Find(nil).Sort("+") }
		f3 := func() { coll.Find(nil).Sort("foo", "-") }

		for _, f := range []func(){f1, f2, f3} {
			So(f, ShouldPanicWith, "Sort: empty field name")
		}
	})
}

func TestQuery_SortScoreText(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		session := ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndex(Index{
			Key: []string{"$text:a", "$text:b"},
		})
		msg := "text search not enabled"
		if err != nil && strings.Contains(err.Error(), msg) {
			t.Skip(msg)
		}
		So(err, ShouldBeNil)

		err = coll.Insert(M{
			"a": "none",
			"b": "twice: foo foo",
		})
		So(err, ShouldBeNil)
		err = coll.Insert(M{
			"a": "just once: foo",
			"b": "none",
		})
		So(err, ShouldBeNil)
		err = coll.Insert(M{
			"a": "many: foo foo foo",
			"b": "none",
		})
		So(err, ShouldBeNil)
		err = coll.Insert(M{
			"a": "none",
			"b": "none",
			"c": "ignore: foo",
		})
		So(err, ShouldBeNil)

		query := coll.Find(M{"$text": M{"$search": "foo"}})
		query.Select(M{"score": M{"$meta": "textScore"}})
		query.Sort("$textScore:score")
		iter := query.Iter()

		var r struct{ A, B string }
		var results []string
		for iter.Next(&r) {
			results = append(results, r.A, r.B)
		}

		So(results, ShouldResemble, []string{
			"many: foo foo foo", "none",
			"none", "twice: foo foo",
			"just once: foo", "none",
		})
	})
}

func TestQuery_Explain(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		ns := []int{40, 41, 42}
		for _, n := range ns {
			err := coll.Insert(M{"n": n})
			So(err, ShouldBeNil)
		}

		m := M{}
		query := coll.Find(nil).Limit(2)
		err = query.Explain(m)
		So(err, ShouldBeNil)
		if m["queryPlanner"] != nil {
			So(m["executionStats"].(M)["totalDocsExamined"], ShouldEqual, 2)
		} else {
			So(m["cursor"], ShouldEqual, "BasicCursor")
			So(m["nscanned"], ShouldEqual, 2)
			So(m["n"], ShouldEqual, 2)
		}

		n := 0
		var result M
		iter := query.Iter()
		for iter.Next(&result) {
			n++
		}
		So(iter.Close(), ShouldBeNil)
		So(n, ShouldEqual, 2)
	})
}
func TestQuery_CountHint(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1})
		So(err, ShouldBeNil)

		_, err = coll.Find(M{"n": M{"$gt": 1}}).Hint("does_not_exists").Count()
	})
}
