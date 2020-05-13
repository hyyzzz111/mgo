package bson

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestObjectIdHex(t *testing.T){
	Convey("TestObjectIdHex",t, func() {
		Convey("Test Should Panic", func() {
			So(func(){ObjectIdHex("!122")},ShouldPanic)
			So(func(){ObjectIdHex("")},ShouldPanic)
			oc,err:= ObjectIDFromHex("")
			So(err,ShouldBeError)
			So(oc,ShouldEqual,NilObjectID)
		})
	})
}