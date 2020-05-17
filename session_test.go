package mgo

import (
	"github.com/smartystreets/assertions/should"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"strings"
	"testing"
)

func TestSession_Dial(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		So(ctx.mongo.Ping(), ShouldBeNil)
	})
}

func TestSession_Ping(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		err := ctx.mongo.Ping()
		So(err, ShouldBeNil)
	})
}
func TestSession_BuildInfo(t *testing.T) {
	MongoTest(t, func(ctx *TestContext) {
		var err error
		var session = ctx.mongo
		info, err := session.BuildInfo()
		So(err, ShouldBeNil)

		var v []int
		for i, a := range strings.Split(info.Version, ".") {
			for _, token := range []string{"-rc", "-pre"} {
				if i == 2 && strings.Contains(a, token) {
					a = a[:strings.Index(a, token)]
					info.VersionArray[len(info.VersionArray)-1] = 0
				}
			}
			n, err := strconv.Atoi(a)
			So(err, ShouldBeNil)
			v = append(v, n)
		}
		for len(v) < 4 {
			v = append(v, 0)
		}

		So(info.VersionArray, ShouldResemble, v)
		So(info.GitVersion, ShouldMatch, "[a-z0-9]+")
		So(info.Bits, ShouldBeIn, []int{64, 32})
		So(info.MaxObjectSize, should.BeGreaterThanOrEqualTo, 8192)

	})
}
