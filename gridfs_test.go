package mgo

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

func TestGridFS_Create(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		db := session.DB("mydb")
		gfs := db.GridFS("fs")
		Convey("test create gridfs", func() {
			file, err := gfs.Create("")
			So(err, ShouldBeNil)
			n, err := file.Write([]byte("abc"))
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 3)
			err = file.Close()
			So(err, ShouldBeNil)
			result := bson.M{}
			err = db.C("fs.files").Find(nil).One(result)
			So(err, ShouldBeNil)
			fileId, ok := result["_id"].(bson.ObjectId)
			So(ok, ShouldBeTrue)
			So(fileId.IsZero(), ShouldBeFalse)
			result["_id"] = "<id>"
			//
			ud, ok := result["uploadDate"].(primitive.DateTime)
			So(ok, ShouldBeTrue)
			So(ud.Time().IsZero(), ShouldBeFalse)
			err = db.C("fs.chunks").Find(nil).One(result)
			So(err, ShouldBeNil)

		})
	})
}
func TestGridFS_FileDetails(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		db := session.DB("mydb")

		gfs := db.GridFS("fs")
		file, err := gfs.Create("myfile1.txt")
		So(err, ShouldBeNil)
		n, err := file.Write([]byte("some"))
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 4)
		So(file.Size(), ShouldEqual, 4)
		n, err = file.Write([]byte(" data"))
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 5)
		So(file.Size(), ShouldEqual, 9)
		id, _ := file.Id().(bson.ObjectId)
		So(id.IsZero(), ShouldBeFalse)
		So(file.Name(), ShouldEqual, "myfile1.txt")
		err = file.Close()
		So(err, ShouldBeNil)

	})
}

func TestGridFS_RemoveId(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		db := session.DB("mydb")
		gfs := db.GridFS("fs")
		file, err := gfs.Create("myfile.txt")
		So(err, ShouldBeNil)
		_, _ = file.Write([]byte{'1'})
		err = file.Close()
		So(err, ShouldBeNil)
		file, err = gfs.Create("myfile.txt")
		So(err, ShouldBeNil)
		_, _ = file.Write([]byte{'2'})
		id := file.Id()
		_ = file.Close()
		err = gfs.RemoveId(id)
		So(err, ShouldBeNil)
		file, err = gfs.Open("myfile.txt")
		So(err, ShouldBeNil)
		defer file.Close()
		var b [1]byte
		_, err = file.Read(b[:])
		So(err, ShouldBeNil)
		So(string(b[:]), ShouldEqual, "1")
		n, err := db.C("fs.chunks").Find(bson.M{"files_id": id}).Count()
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 0)

	})
}
func TestGridFS_Remove(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		session := ctx.mongo
		db := session.DB("mydb")

		gfs := db.GridFS("fs")

		file, err := gfs.Create("myfile.txt")
		So(err, ShouldBeNil)
		_, _ = file.Write([]byte{'1'})
		_ = file.Close()

		file, err = gfs.Create("myfile.txt")
		So(err, ShouldBeNil)
		_, _ = file.Write([]byte{'2'})
		_ = file.Close()

		err = gfs.Remove("myfile.txt")
		So(err, ShouldBeNil)

		_, err = gfs.Open("myfile.txt")
		So(err == ErrNotFound, ShouldEqual, true)

		n, err := db.C("fs.chunks").Find(nil).Count()
		So(err, ShouldBeNil)
		So(n, ShouldEqual, 0)
	})
}
