package bson

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestBSON_ObjectIdHex(t *testing.T) {
	Convey("", t, func() {
		So(func() { ObjectIdHex("!122") }, ShouldPanic)
		So(func() { ObjectIdHex("") }, ShouldPanic)
		oc, err := ObjectIDFromHex("")
		So(err, ShouldBeError)
		So(oc, ShouldEqual, NilObjectID)
	})
}
