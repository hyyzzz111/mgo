package mgo

import (
	"github.com/davecgh/go-spew/spew"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestDatabase_CollectionNames(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		Convey("Test CollectionNames", func() {
			dbNames,err:=ctx.mongo.DatabaseNames()
			So(err,ShouldBeNil)
			So(len(dbNames),ShouldEqual,3)
			So(dbNames,ShouldResemble,[]string{
				"admin","config","local",
			})
			spew.Dump()

		})
	})
}