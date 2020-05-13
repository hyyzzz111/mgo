package mgo

import (
	"context"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"testing"
)

const (
	TestDbName = "mgo_test"
)

var (
	TestCtx = context.TODO()
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
	ctx := context.Background()
	mongoC, err := GetDockerMongoC(ctx, "", "")
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
	s, err := Dial("mongodb://" + ipAdd + "/mgo_test")
	return s, mongoC, func() {
		mongoC.Terminate(ctx)
	}, err
}

type TestContext struct {
	context.Context
	mongo  *Session
	mongoC testcontainers.Container
}

func MongoTest(t *testing.T, fn func(ctx *TestContext)) {
	Convey("Boot Mongo By Docker", t, func() {
		ms, mc, cancel, err := session()
		So(err, ShouldBeNil)
		fn(&TestContext{mongo: ms, mongoC: mc,Context:context.Background()})

		Reset(cancel)
	})
}
