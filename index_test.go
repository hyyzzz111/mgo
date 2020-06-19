package mgo

import (
	"flag"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/yaziming/mgo/bson"
	"strings"
	"testing"
	"time"
)

var indexTests = []struct {
	index    Index
	expected M
}{{
	Index{
		Key:        []string{"a"},
		Background: true,
	},
	M{
		"background": true,
		"key":        M{"a": int32(1)},
		"name":       "a_1", "ns": "mydb.mycoll",
	},
}, {
	Index{
		Key:  []string{"@loc_old"}, // Obsolete
		Minf: -500,
		Maxf: 500,
		Bits: 32,
	},
	M{
		"name": "loc_old_2d",
		"key":  M{"loc_old": "2d"},
		"ns":   "mydb.mycoll",
		"min":  float64(-500.0),
		"max":  float64(500.0),
		"bits": int32(32),
	},
}, {
	Index{
		Key:  []string{"$2d:loc"},
		Minf: -500,
		Maxf: 500,
		Bits: 32,
	},
	M{
		"name": "loc_2d",
		"key":  M{"loc": "2d"},
		"ns":   "mydb.mycoll",
		"min":  float64(-500.0),
		"max":  float64(500.0),
		"bits": int32(32),
	},
}, {
	Index{
		Key:  []string{"$2d:loc"},
		Minf: -500.1,
		Maxf: 500.1,
		Bits: 32,
	},
	M{
		"name": "loc_2d",
		"key":  M{"loc": "2d"},
		"ns":   "mydb.mycoll",
		"min":  -500.1,
		"max":  500.1,
		"bits": 32,
	},
}, {
	Index{
		Key:        []string{"$geoHaystack:loc", "type"},
		BucketSize: 1,
	},
	M{
		"name":       "loc_geoHaystack_type_1",
		"key":        M{"loc": "geoHaystack", "type": 1},
		"ns":         "mydb.mycoll",
		"bucketSize": 1.0,
	},
}, {
	Index{
		Key:     []string{"$text:a", "$text:b"},
		Weights: map[string]int{"b": 42},
	},
	M{
		"name":              "a_text_b_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"a": 1, "b": 42},
		"default_language":  "english",
		"language_override": "language",
		"textIndexVersion":  2,
	},
}, {
	Index{
		Key:              []string{"$text:a"},
		DefaultLanguage:  "portuguese",
		LanguageOverride: "idioma",
	},
	M{
		"name":              "a_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"a": 1},
		"default_language":  "portuguese",
		"language_override": "idioma",
		"textIndexVersion":  2,
	},
}, {
	Index{
		Key: []string{"$text:$**"},
	},
	M{
		"name":              "$**_text",
		"key":               M{"_fts": "text", "_ftsx": 1},
		"ns":                "mydb.mycoll",
		"weights":           M{"$**": 1},
		"default_language":  "english",
		"language_override": "language",
		"textIndexVersion":  2,
	},
}, {
	Index{
		Key:  []string{"cn"},
		Name: "CustomName",
	},
	M{
		"name": "CustomName",
		"key":  M{"cn": 1},
		"ns":   "mydb.mycoll",
	},
}, {
	Index{
		Key: []string{"partial"},
		PartialFilter: bson.M{
			"b": bson.M{"$gt": int32(42)},
		},
	},
	M{
		"name": "partial_1",
		"ns":   "mydb.mycoll",
		"partialFilterExpression": M{
			"b": M{"$gt": int32(42)},
		},
		"key": M{"partial": 1},
	},
},
}

func TestIndex_EnsureIndex(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		for _, test := range indexTests {
			t.Log(test.expected["name"])
			err = coll.EnsureIndex(test.index)
			msg := "text search not enabled"
			if err != nil && strings.Contains(err.Error(), msg) {
				continue
			}
			So(err, ShouldBeNil)

			expectedName := test.index.Name
			if expectedName == "" {
				expectedName, _ = test.expected["name"].(string)
			}

			indexes, err := coll.Indexes()
			So(err, ShouldBeNil)
			So(indexes, ShouldHaveLength, 2)
			gotIndex := indexes[0]
			if gotIndex.Name == "_id_" {
				gotIndex = indexes[1]
			}
			wantIndex := test.index
			if wantIndex.Name == "" {
				wantIndex.Name = gotIndex.Name
			}
			if strings.HasPrefix(wantIndex.Key[0], "@") {
				wantIndex.Key[0] = "$2d:" + wantIndex.Key[0][1:]
			}

			if wantIndex.DefaultLanguage == "" {
				wantIndex.DefaultLanguage = gotIndex.DefaultLanguage
			}
			if wantIndex.LanguageOverride == "" {
				wantIndex.LanguageOverride = gotIndex.LanguageOverride
			}

			if len(wantIndex.PartialFilter) == 0 {
				wantIndex.PartialFilter = gotIndex.PartialFilter
			}
			for name := range gotIndex.Weights {
				if _, ok := wantIndex.Weights[name]; !ok {
					if wantIndex.Weights == nil {
						wantIndex.Weights = make(map[string]int)
					}
					wantIndex.Weights[name] = 1
				}
			}
			So(gotIndex, ShouldResemble, wantIndex)

			// Drop created index by key or by name if a custom name was used.
			if test.index.Name == "" {
				err = coll.DropIndex(test.index.Key...)
				So(err, ShouldBeNil)
			} else {
				err = coll.DropIndexName(test.index.Name)
				So(err, ShouldBeNil)
			}
		}
	})

}

func TestIndex_EnsureIndexWithBadInfo(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndex(Index{})
		So(err, ShouldErrorMatche, "invalid index key:.*")

		err = coll.EnsureIndex(Index{Key: []string{""}})
		So(err, ShouldErrorMatche, "invalid index key:.*")
	})

}

func TestIndex_EnsureIndexKey(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndexKey("a")
		So(err, ShouldBeNil)

	})

}

func TestIndex_EnsureIndexDropIndex(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndexKey("a")
		So(err, ShouldBeNil)

		err = coll.EnsureIndexKey("-b")
		So(err, ShouldBeNil)

		err = coll.DropIndex("-b")
		So(err, ShouldBeNil)

		err = coll.DropIndex("a")
		So(err, ShouldBeNil)

		// Try to drop it again
		err = coll.DropIndex("a")
		So(err, ShouldErrorMatche, "index not found.*")

	})

}

func TestIndex_EnsureIndexDropIndexName(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndexKey("a")
		So(err, ShouldBeNil)

		err = coll.EnsureIndex(Index{Key: []string{"b"}, Name: "a"})
		So(err, ShouldBeNil)

		err = coll.DropIndexName("a")
		So(err, ShouldBeNil)

		err = coll.DropIndexName("a_1")
		So(err, ShouldBeNil)

		err = coll.DropIndexName("a_1")
		So(err, ShouldErrorMatche, "index not found.*")

	})

}

func TestIndex_EnsureIndexDropAllIndexes(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndexKey("a")
		So(err, ShouldBeNil)

		err = coll.EnsureIndexKey("b")
		So(err, ShouldBeNil)

		err = coll.DropAllIndexes()
		So(err, ShouldBeNil)

	})

}

func TestIndex_EnsureIndexGetIndexes(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		coll := session.DB("mydb").C("mycoll")

		err = coll.EnsureIndexKey("-b")
		So(err, ShouldBeNil)

		err = coll.EnsureIndexKey("a")
		So(err, ShouldBeNil)

		// Obsolete.
		err = coll.EnsureIndexKey("@c")
		So(err, ShouldBeNil)

		err = coll.EnsureIndexKey("$2d:d")
		So(err, ShouldBeNil)

		indexes, err := coll.Indexes()
		So(err, ShouldBeNil)
		So(indexes[0].Name, ShouldEqual, "_id_")
		So(indexes[1].Name, ShouldEqual, "a_1")
		So(indexes[1].Key, ShouldResemble, []string{"a"})
		So(indexes[2].Name, ShouldEqual, "b_-1")
		So(indexes[2].Key, ShouldResemble, []string{"-b"})
		So(indexes[3].Name, ShouldEqual, "c_2d")
		So(indexes[3].Key, ShouldResemble, []string{"$2d:c"})
		So(indexes[4].Name, ShouldEqual, "d_2d")
		So(indexes[4].Key, ShouldResemble, []string{"$2d:d"})
	})

}

var testTTL = flag.Bool("test-ttl", false, "test TTL collections (may take 1 minute)")

func TestIndex_EnsureIndexExpireAfter(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo

		coll := session.DB("mydb").C("mycoll")

		err = coll.Insert(M{"n": 1, "t": time.Now().Add(-120 * time.Second)})
		So(err, ShouldBeNil)
		err = coll.Insert(M{"n": 2, "t": time.Now()})
		So(err, ShouldBeNil)

		// Should fail since there are duplicated entries.
		index := Index{
			Key:         []string{"t"},
			ExpireAfter: 1 * time.Minute,
		}

		err = coll.EnsureIndex(index)
		So(err, ShouldBeNil)

		indexes, err := coll.Indexes()
		So(err, ShouldBeNil)
		So(indexes[1].Name, ShouldEqual, "t_1")
		So(indexes[1].ExpireAfter, ShouldEqual, 1*time.Minute)

		if *testTTL {
			worked := false
			stop := time.Now().Add(70 * time.Second)
			for time.Now().Before(stop) {
				n, err := coll.Count()
				So(err, ShouldBeNil)
				if n == 1 {
					worked = true
					break
				}
				So(n, ShouldEqual, 2)
				t.Logf("Still has 2 entries...")
				time.Sleep(1 * time.Second)
			}
			if !worked {
				t.Fatalf("TTL index didn't work")
			}
		}
	})

}
