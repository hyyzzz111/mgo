package mgo

import (
	"github.com/smartystreets/assertions/should"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/yaziming/mgo/bson"

	"testing"
)

func TestDatabase_Find(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		Convey("Test CollectionNames", func() {
			i, err := ctx.mongoC.Exec(ctx, []string{
				"mongo",
				"--eval",
				"db.items.insertMany([\n      {  item: \"large box\", qty: 20, user: ObjectId() },\n      {  item: \"small box\", qty: 55,user: ObjectId() },\n      { _id:ObjectId(\"5ebac68c2211678e6c303019\"), item: \"medium box\", qty: 30, user: ObjectId()},\n      {  item: \"medium1 box\", qty: 35, user: \"\"},\n      {  item: \"medium2 box\", qty: 35},\n])",			})
			So(err, ShouldBeNil)
			So(i, ShouldEqual, 0)
			session := ctx.mongo
			c := session.C("items")
			type test struct {
				Id     bson.ObjectId `json:"_id" bson:"_id,omitempty"`
				Qty    int           `json:"qty"`
				UserId bson.ObjectId `json:"user_id" bson:"user_id"`
			}
			Convey("Test Find Where nil", func() {
				var item test
				err = c.Find(nil).One(&item)
				So(err, ShouldBeNil)
				So(item, ShouldNotEqual, test{})
			})
			Convey("Test One Func Should Auto Fill Mongo Don`t Exists Field To Mongo ObjectID", func() {
				var item test
				err = c.Find(bson.M{
					"item": "medium2 box",
				}).One(&item)
				So(err, ShouldBeNil)
				So(item.UserId, ShouldEqual, bson.NilObjectID)
				So(item.Id, ShouldNotEqual, bson.NilObjectID)
			})
			Convey("Test Find All", func() {
				var item []test
				err = c.Find(bson.M{
					"item": "medium2 box",
				}).All(&item)
				So(err, ShouldBeNil)
				So(len(item), ShouldEqual, 1)
				So(item[0].UserId, ShouldEqual, bson.NilObjectID)
				So(item[0].Id, ShouldNotEqual, bson.NilObjectID)
				err = c.Find(nil).All(&item)
				So(err, ShouldBeNil)
				So(len(item), ShouldEqual, 5)
			})
			Convey("Test FindId", func() {
				var result test
				err :=c.FindId("5ebac68c2211678e6c303019").One(&result)
				So(err,ShouldBeNil)
				err =c.FindId(bson.ObjectIdHex("5ebac68c2211678e6c303019")).One(&result)
				So(err,ShouldBeNil)
			})
			Convey("Test Find And Apply", func() {
				change := Change{
					Update: bson.M{
						"$set": bson.M{
							"item": "medium2 box1",
						},
					},
					Upsert:    false,
					Remove:    false,
					ReturnNew: true,
				}
				var result test
				info, err := c.Find(bson.M{"item": "medium2 box"}).Apply(change, &result)
				So(err, ShouldBeNil)
				So(info.Updated, ShouldEqual, 1)
			})
			Convey("delete all document", func() {
				ch,err:=c.RemoveAll(nil)
				So(err,ShouldBeNil)
				So(ch.Removed,ShouldBeGreaterThan,1)
			})
			Convey("Drop Collection", func() {
				err :=c.Drop()
				So(err,ShouldBeNil)
				collections,err:=session.DatabaseNames()
				So(err,ShouldBeNil)
				So(collections,should.NotContain,"items")
			})
		})
	})
}

func Test2(t *testing.T){
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		c := session.C("items")
		doc := bson.M{
			"name":"1",
		}
		err:=c.Insert(doc)
		So(err,ShouldBeNil)
		count,err:=c.Count(nil)
		So(err,ShouldBeNil)
		So(count,ShouldEqual,1)
		err =c.Update(bson.M{"name":"1"},bson.M{"w":1})
		So(err,ShouldBeNil)
		err =c.Update(bson.M{"w":1},bson.M{"$set":bson.M{"age":1}})
		So(err,ShouldBeNil)

		count,err = c.Find(bson.M{"age":1,"w":1}).Count()
		So(err,ShouldBeNil)
		So(count,ShouldEqual,1)
		_=c.Insert(bson.M{"a":1},bson.M{"a":2})
		err = c.Update(nil,bson.M{"$set":bson.M{"d":1}},true)
		So(err,ShouldBeNil)
		count,err = c.Find(bson.M{"d":1}).Count()
		So(err,ShouldBeNil)
		So(count,ShouldEqual,1)
	})
}
