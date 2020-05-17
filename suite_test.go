package mgo

import (
	"context"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"log"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"testing"
)

func GetDockerMongoC(ctx context.Context, username string, password string) (container testcontainers.Container, err error) {
	req := testcontainers.ContainerRequest{
		Image:        "mongo",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForListeningPort("27017/tcp"),
	}
	if username != "" && password != "" {
		req.Env = map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": username,
			"MONGO_INITDB_ROOT_PASSWORD": password,
		}
	}
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
}
func session() (session *Session, c testcontainers.Container, cancel func(), err error) {
	return authSession("", "")
}
func authSession(username string, password string) (session *Session, c testcontainers.Container, cancel func(), err error) {
	ctx := context.Background()
	mongoC, err := GetDockerMongoC(ctx, username, password)

	if err != nil {
		return
	}
	ip, err := mongoC.Host(ctx)
	if err != nil {
		return
	}
	natPort, err := mongoC.MappedPort(ctx, "27017/tcp")
	if err != nil {
		return
	}
	ipAdd := net.JoinHostPort(ip, natPort.Port())
	s, err := Dial("mongodb://" + ipAdd + "/test")
	return s, mongoC, func() {
		_ = mongoC.Terminate(ctx)
	}, err
}

type TestContext struct {
	context.Context
	mongo  *Session
	mongoC testcontainers.Container
}

func MongoTest(t *testing.T, fn func(ctx *TestContext)) {
	log.SetOutput(ioutil.Discard)
	Convey("test mongo suites by docker", t, FailureHalts, func() {
		ms, mc, cancel, err := session()
		So(err, ShouldBeNil)
		fn(&TestContext{mongo: ms, mongoC: mc, Context: context.Background()})

		Reset(func() {
			cancel()
		})
	})
}
func AuthMongoTest(t *testing.T, fn func(ctx *TestContext)) {
	log.SetOutput(ioutil.Discard)
	Convey("test security mongo suites by docker", t, FailureHalts, func() {
		ms, mc, cancel, err := authSession("crawlab", "crawlab_mgo")
		So(err, ShouldBeNil)
		fn(&TestContext{mongo: ms, mongoC: mc, Context: context.Background()})

		Reset(func() {
			cancel()
		})
	})
}
