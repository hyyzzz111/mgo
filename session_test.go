package mgo

import (
	"context"
	. "github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
)


func TestDial(t *testing.T) {
	ctx := context.Background()
	mongoC, err := GetDockerMongoC(ctx, "", "")
	if err != nil {
		t.Error(err)
	}
	ip, err := mongoC.Host(ctx)
	if err != nil {
		t.Error(err)
	}
	natPort, err := mongoC.MappedPort(ctx, "27017/tcp")
	if err != nil {
		t.Error(err)
	}
	ipAdd := net.JoinHostPort(ip, natPort.Port())
	Convey("test session dial", t, func() {
		s, err := Dial("mongodb://" + ipAdd + "/mgo_test")
		So(err, ShouldBeNil)
		So(s.Ping(), ShouldBeNil)
		//So(s.N)
		Reset(func() {
			mongoC.Terminate(ctx)
		})
	})
}

func TestSession_Ping(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		Convey("Ping", func() {
			err := ctx.mongo.Ping()
			So(err, ShouldBeNil)
		})
	})
}
