package mgo

import (
	"context"
	"github.com/Masterminds/semver"
	"github.com/yaziming/mgo/bson"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var (
	// ErrNotFound multierror returned when a document could not be found
	ErrNotFound = mongo.ErrNoDocuments
	// ErrCursor multierror returned when trying to retrieve documents from
	// an invalid cursor
	//ErrCursor = errors.New("invalid cursor")
)

// ReadPreference defines the manner in which servers are chosen.
type ReadPreference struct {
	// Mode determines the consistency of results. See Session.SetMode.
	Mode Mode

	// TagSets indicates which servers are allowed to be used. See Session.SelectServers.
	TagSets []bson.D
}

// Session session session
type Session struct {
	client    *mongo.Client
	database  string
	uri       string
	m         sync.RWMutex
	buildInfo BuildInfo
}

func (s *Session) Run(cmd interface{}, result interface{}) error {
	return s.DB("admin").Run(cmd, result)
}

// New session
//
// Relevant documentation:
//
// 		https://docs.mongodb.com/manual/reference/connection-string/
func New(uri string) *Session {
	session := &Session{
		uri: uri,
	}
	return session
}
func Dial(url string) (*Session, error) {
	if !strings.HasPrefix(url, "mongodb://") && !strings.HasPrefix(url, "mongodb+srv://") {
		url = "mongodb://" + url
	}
	session := New(url)
	err := session.Connect()

	return session, err
}
func NewFromMongoDriver(m *mongo.Client, database string) *Session {
	session := &Session{
		database: database,
		client:   m,
	}
	return session
}

func (s *Session) Close() {
}

// Collection returns coll
func (s *Session) C(collection string) *Collection {
	s.m.Lock()
	defer s.m.Unlock()
	return s.DB(s.database).C(collection)
}

// Connect session client
func (s *Session) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opt := options.Client().SetSocketTimeout(1 * time.Minute).SetConnectTimeout(1 * time.Minute).ApplyURI(s.uri)
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return err
	}
	connectionString, err := connstring.ParseAndValidate(s.uri)
	if err != nil {
		return err
	}
	s.database = connectionString.Database
	s.client = client
	s.buildInfo, err = s.BuildInfo()
	if err != nil {
		return err
	}
	return nil
}

// Ping verifies that the client can connect to the topology.
// If readPreference is nil then will use the client's default read
// preference.
func (s *Session) Ping() error {
	return s.client.Ping(context.TODO(), readpref.Primary())
}

// DB returns a value representing the named db.
func (s *Session) DB(db string) *Database {
	version, _ := semver.NewVersion(s.buildInfo.Version)
	return &Database{version: version, database: s.client.Database(db)}
}

type BuildInfo struct {
	Version        string
	VersionArray   []int  `bson:"versionArray"` // On MongoDB 2.0+; assembled from Version otherwise
	GitVersion     string `bson:"gitVersion"`
	OpenSSLVersion string `bson:"OpenSSLVersion"`
	Bits           int
	Debug          bool
	MaxObjectSize  int `bson:"maxBsonObjectSize"`
}
type ChangeInfo struct {
	// Updated reports the number of existing documents modified.
	// Due to server limitations, this reports the same value as the Matched field when
	// talking to MongoDB <= 2.4 and on Upsert and Apply (findAndModify) operations.
	Updated    int
	Removed    int         // Number of documents removed
	Matched    int         // Number of documents matched but not necessarily changed
	UpsertedId interface{} // Upserted _id field, when not explicitly provided
}

func (s *Session) BuildInfo() (info BuildInfo, err error) {

	result := s.client.Database("admin").RunCommand(context.Background(), bson.M{"buildInfo": "1"})
	err = result.Decode(&info)
	if err != nil {
		return
	}

	//err = s.Run(bson.D{{Name: "buildInfo", Value: "1"}}, &info)
	if len(info.VersionArray) == 0 {
		for _, a := range strings.Split(info.Version, ".") {
			i, err := strconv.Atoi(a)
			if err != nil {
				break
			}
			info.VersionArray = append(info.VersionArray, i)
		}
	}
	for len(info.VersionArray) < 4 {
		info.VersionArray = append(info.VersionArray, 0)
	}
	if i := strings.IndexByte(info.GitVersion, ' '); i >= 0 {
		// Strip off the " modules: enterprise" suffix. This is a _git version_.
		// That information may be moved to another field if people need it.
		info.GitVersion = info.GitVersion[:i]
	}

	return
}

func (s *Session) DatabaseNames() (names []string, err error) {
	return s.client.ListDatabaseNames(context.Background(), bson.M{}, options.ListDatabases().SetNameOnly(true))
}

func (s *Session) Copy() *Session {
	s.m.Lock()
	defer s.m.Unlock()
	return &Session{
		client:   s.client,
		database: s.database,
		uri:      s.uri,
		m:        sync.RWMutex{},
	}
}

// DialInfo holds options for establishing a session with a MongoDB cluster.
// To use a URL, see the Dial function.
type DialInfo struct {
	// Addrs holds the addresses for the seed servers.
	Addrs []string

	// Timeout is the amount of time to wait for a server to respond when
	// first connecting and on follow up operations in the session. If
	// timeout is zero, the call may block forever waiting for a connection
	// to be established. Timeout does not affect logic in DialServer.
	Timeout time.Duration

	// Database is the default db name used when the Session.DB method
	// is called with an empty name, and is also used during the initial
	// authentication if Source is unset.
	Database string

	// ReplicaSetName, if specified, will prevent the obtained session from
	// communicating with any server which is not part of a replica set
	// with the given name. The default is to communicate with any server
	// specified or discovered via the servers contacted.
	ReplicaSetName string

	// Source is the db used to establish credentials and privileges
	// with a MongoDB server. Defaults to the value of Database, if that is
	// set, or "admin" otherwise.
	Source string

	// Service defines the service name to use when authenticating with the GSSAPI
	// mechanism. Defaults to "mongodb".
	Service string

	// ServiceHost defines which hostname to use when authenticating
	// with the GSSAPI mechanism. If not specified, defaults to the MongoDB
	// server's address.
	ServiceHost string

	// Mechanism defines the protocol for credential negotiation.
	// Defaults to "MONGODB-CR".
	Mechanism string

	// Username and Password inform the credentials for the initial authentication
	// done on the db defined by the Source field. See Session.Login.
	Username string
	Password string

	// PoolLimit defines the per-server socket pool limit. Defaults to
	// DefaultConnectionPoolLimit. See Session.SetPoolLimit for details.
	PoolLimit int

	// PoolTimeout defines max time to wait for a connection to become available
	// if the pool limit is reached. Defaults to zero, which means forever. See
	// Session.SetPoolTimeout for details
	PoolTimeout time.Duration

	// ReadTimeout defines the maximum duration to wait for a response to be
	// read from MongoDB.
	//
	// This effectively limits the maximum query execution time. If a MongoDB
	// query duration exceeds this timeout, the caller will receive a timeout,
	// however MongoDB will continue processing the query. This duration must be
	// large enough to allow MongoDB to execute the query, and the response be
	// received over the network connection.
	//
	// Only limits the network read - does not include unmarshalling /
	// processing of the response. Defaults to DialInfo.Timeout. If 0, no
	// timeout is set.
	ReadTimeout time.Duration

	// WriteTimeout defines the maximum duration of a write to MongoDB over the
	// network connection.
	//
	// This is can usually be low unless writing large documents, or over a high
	// latency link. Only limits network write time - does not include
	// marshalling/processing the request. Defaults to DialInfo.Timeout. If 0,
	// no timeout is set.
	WriteTimeout time.Duration

	// The identifier of the client application which ran the operation.
	AppName string

	// ReadPreference defines the manner in which servers are chosen. See
	// Session.SetMode and Session.SelectServers.
	ReadPreference *ReadPreference

	// Safe mostly defines write options, though there is RMode. See Session.SetSafe
	Safe Safe

	// FailFast will cause connection and query attempts to fail faster when
	// the server is unavailable, instead of retrying until the configured
	// timeout period. Note that an unavailable server may silently drop
	// packets instead of rejecting them, in which case it's impossible to
	// distinguish it from a slow server, so the timeout stays relevant.
	FailFast bool

	// Direct informs whether to establish connections only with the
	// specified seed servers, or to obtain information for the whole
	// cluster and establish connections with further servers too.
	Direct bool

	// MinPoolSize defines The minimum number of connections in the connection pool.
	// Defaults to 0.
	MinPoolSize int

	// The maximum number of milliseconds that a connection can remain idle in the pool
	// before being removed and closed.
	MaxIdleTimeMS int
	DialServer    func(addr *ServerAddr) (net.Conn, error)
}
type ServerAddr struct {
	str string
	tcp *net.TCPAddr
}

// DialWithInfo establishes a new session to the cluster identified by info.
func DialWithInfo(dialInfo *DialInfo) (session *Session, err error) {

	defaultOptions := options.Client().
		SetMinPoolSize(uint64(dialInfo.MinPoolSize)).
		SetMaxConnIdleTime(time.Duration(dialInfo.MaxIdleTimeMS)).
		SetAppName(dialInfo.AppName).SetConnectTimeout(dialInfo.Timeout)
	socketTimeout := dialInfo.ReadTimeout
	if dialInfo.WriteTimeout > socketTimeout {
		socketTimeout = dialInfo.WriteTimeout
	}
	if socketTimeout > 0 {
		defaultOptions.SetSocketTimeout(dialInfo.ReadTimeout)
	}
	if dialInfo.PoolLimit > 0 {
		defaultOptions.SetMaxPoolSize(uint64(dialInfo.PoolLimit))
	}
	if !dialInfo.FailFast {
		defaultOptions.SetRetryReads(true)
		defaultOptions.SetRetryWrites(true)
	}
	if dialInfo.ReadPreference != nil {
		panic("dialInfo.ReadPreference dont implatement")
	}
	addrs := make([]string, len(dialInfo.Addrs))
	copy(addrs, dialInfo.Addrs)
	defaultOptions.SetHosts(addrs)
	if dialInfo.Username != "" && dialInfo.Password != "" {
		defaultOptions.SetAuth(options.Credential{
			AuthSource:  dialInfo.Source,
			Username:    dialInfo.Username,
			Password:    dialInfo.Password,
			PasswordSet: false,
		})
	}
	if dialInfo.DialServer != nil {
		var dialer topology.DialerFunc
		dialer = func(ctx context.Context, network, address string) (net.Conn, error) {
			tcpAddr, err := resolveAddr(address)
			if err != nil {
				return nil, err
			}
			return dialInfo.DialServer(&ServerAddr{
				str: address,
				tcp: tcpAddr,
			})
		}
		defaultOptions.SetDialer(dialer)
	}
	client, err := mongo.Connect(context.TODO(), defaultOptions)
	if err != nil {
		return
	}
	session = NewFromMongoDriver(client, dialInfo.Database)
	return session, nil
}
