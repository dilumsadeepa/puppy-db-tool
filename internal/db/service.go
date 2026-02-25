package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"puppy-db-tool-desktop/internal/model"
)

type session struct {
	conn        model.Connection
	sqlDB       *sql.DB
	mongoClient *mongo.Client
	mongoDB     string
	redisClient *redis.Client
	tunnel      *Tunnel
}

type Service struct {
	mu       sync.RWMutex
	sessions map[string]*session
}

func NewService() *Service {
	return &Service{
		sessions: map[string]*session{},
	}
}

func (s *Service) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.sessions {
		s.closeSession(sess)
	}
	s.sessions = map[string]*session{}
}

func (s *Service) TestConnection(conn model.Connection) error {
	sess, err := s.openSession(conn)
	if err != nil {
		return err
	}
	s.closeSession(sess)
	return nil
}

func (s *Service) Connect(conn model.Connection) error {
	sess, err := s.openSession(conn)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.sessions[conn.ID]; ok {
		s.closeSession(existing)
	}
	s.sessions[conn.ID] = sess
	return nil
}

func (s *Service) Disconnect(connID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[connID]; ok {
		s.closeSession(sess)
		delete(s.sessions, connID)
	}
}

func (s *Service) ConnectionType(connID string) (model.DBType, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return "", err
	}
	return sess.conn.Type, nil
}

func (s *Service) RunQuery(connID string, query string) (model.QueryResult, time.Duration, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return model.QueryResult{}, 0, err
	}

	start := time.Now()
	result, err := s.runByType(sess, query)
	duration := time.Since(start)
	return result, duration, err
}

func (s *Service) ListObjects(connID string) ([]model.DBObject, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return nil, err
	}
	return s.listObjectsByType(sess)
}

func (s *Service) ListTableInfo(connID, schema string) ([]model.TableInfo, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return nil, err
	}
	return s.listTableInfoByType(sess, schema)
}

func (s *Service) DescribeTableColumns(connID, schema, table string) ([]model.TableColumn, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return nil, err
	}
	return s.describeTableColumnsByType(sess, schema, table)
}

func (s *Service) FetchTableData(connID, schema, table, filter, sort string, limit, offset int) (model.TableData, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return model.TableData{}, err
	}
	return s.fetchTableDataByType(sess, schema, table, filter, sort, limit, offset)
}

func (s *Service) CountTableRows(connID, schema, table, filter string) (int64, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return 0, err
	}
	return s.countRowsByType(sess, schema, table, filter)
}

func (s *Service) InsertRow(connID, schema, table string, row map[string]string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.insertByType(sess, schema, table, row)
}

func (s *Service) UpdateRow(connID, schema, table, keyColumn, keyValue string, row map[string]string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.updateByType(sess, schema, table, keyColumn, keyValue, row)
}

func (s *Service) DeleteRow(connID, schema, table, keyColumn, keyValue string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.deleteByType(sess, schema, table, keyColumn, keyValue)
}

func (s *Service) EmptyTable(connID, schema, table string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.emptyTableByType(sess, schema, table)
}

func (s *Service) DropTable(connID, schema, table string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.dropTableByType(sess, schema, table)
}

func (s *Service) CreateTable(connID, schema, table string) error {
	sess, err := s.getSession(connID)
	if err != nil {
		return err
	}
	return s.createTableByType(sess, schema, table)
}

func (s *Service) ExportDatabase(connID, schema string) (string, error) {
	sess, err := s.getSession(connID)
	if err != nil {
		return "", err
	}
	return s.exportDatabaseByType(sess, schema)
}

func (s *Service) getSession(connID string) (*session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[connID]
	if !ok {
		return nil, errors.New("connection is not active")
	}
	return sess, nil
}

func (s *Service) openSession(conn model.Connection) (*session, error) {
	work := conn
	var tunnel *Tunnel
	if conn.SSH.Enabled {
		host, port, err := resolveRemoteAddress(conn)
		if err != nil {
			return nil, err
		}
		tunnel, localPort, err := StartTunnel(conn.SSH, host, port)
		if err != nil {
			return nil, err
		}
		work, err = applyTunnel(conn, localPort)
		if err != nil {
			tunnel.Close()
			return nil, err
		}
	}

	sess := &session{conn: work, tunnel: tunnel}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	switch work.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		dsn, driver, err := sqlDSN(work)
		if err != nil {
			s.closeSession(sess)
			return nil, err
		}
		db, err := sql.Open(driver, dsn)
		if err != nil {
			s.closeSession(sess)
			return nil, err
		}
		db.SetConnMaxIdleTime(30 * time.Second)
		db.SetConnMaxLifetime(2 * time.Minute)
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(4)
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			s.closeSession(sess)
			return nil, fmt.Errorf("db ping failed: %w", err)
		}
		sess.sqlDB = db

	case model.DBTypeMongo:
		uri, err := mongoURI(work)
		if err != nil {
			s.closeSession(sess)
			return nil, err
		}
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err != nil {
			s.closeSession(sess)
			return nil, err
		}
		if err := client.Ping(ctx, nil); err != nil {
			_ = client.Disconnect(context.Background())
			s.closeSession(sess)
			return nil, err
		}
		sess.mongoClient = client
		sess.mongoDB = readMongoDB(work)

	case model.DBTypeRedis:
		opts, err := redisOptions(work)
		if err != nil {
			s.closeSession(sess)
			return nil, err
		}
		client := redis.NewClient(opts)
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			s.closeSession(sess)
			return nil, err
		}
		sess.redisClient = client

	default:
		s.closeSession(sess)
		return nil, fmt.Errorf("unsupported type: %s", work.Type)
	}

	return sess, nil
}

func (s *Service) closeSession(sess *session) {
	if sess == nil {
		return
	}
	if sess.sqlDB != nil {
		_ = sess.sqlDB.Close()
	}
	if sess.mongoClient != nil {
		_ = sess.mongoClient.Disconnect(context.Background())
	}
	if sess.redisClient != nil {
		_ = sess.redisClient.Close()
	}
	if sess.tunnel != nil {
		sess.tunnel.Close()
	}
}

func resolveRemoteAddress(conn model.Connection) (string, int, error) {
	if conn.Host != "" {
		port := conn.Port
		if port == 0 {
			port = defaultPort(conn.Type)
		}
		return conn.Host, port, nil
	}
	if !conn.UseConnString {
		return "", 0, errors.New("missing host and port")
	}

	switch conn.Type {
	case model.DBTypePostgres, model.DBTypeMongo, model.DBTypeRedis:
		u, err := url.Parse(conn.ConnString)
		if err != nil {
			return "", 0, fmt.Errorf("parse connection string: %w", err)
		}
		host := u.Hostname()
		if host == "" {
			return "", 0, errors.New("host not found in connection string")
		}
		port := defaultPort(conn.Type)
		if p := u.Port(); p != "" {
			parsed, err := strconv.Atoi(p)
			if err != nil {
				return "", 0, err
			}
			port = parsed
		}
		return host, port, nil
	case model.DBTypeMySQL:
		re := regexp.MustCompile(`@tcp\(([^)]+)\)`)
		match := re.FindStringSubmatch(conn.ConnString)
		if len(match) != 2 {
			return "", 0, errors.New("mysql connection string missing @tcp(host:port)")
		}
		host, portText, ok := strings.Cut(match[1], ":")
		if !ok {
			return host, defaultPort(conn.Type), nil
		}
		port, err := strconv.Atoi(portText)
		if err != nil {
			return "", 0, err
		}
		return host, port, nil
	default:
		return "", 0, errors.New("unsupported database type")
	}
}

func applyTunnel(conn model.Connection, localPort int) (model.Connection, error) {
	work := conn
	host := "127.0.0.1"
	work.Host = host
	work.Port = localPort

	if !conn.UseConnString {
		return work, nil
	}

	var updated string
	var err error
	switch conn.Type {
	case model.DBTypePostgres, model.DBTypeMongo, model.DBTypeRedis:
		updated, err = replaceURLHostPort(conn.ConnString, host, localPort)
	case model.DBTypeMySQL:
		updated, err = replaceMySQLTCPHostPort(conn.ConnString, host, localPort)
	default:
		return work, errors.New("unsupported database type")
	}
	if err != nil {
		return work, err
	}
	work.ConnString = updated
	return work, nil
}

func replaceURLHostPort(raw, host string, port int) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Host = fmt.Sprintf("%s:%d", host, port)
	return u.String(), nil
}

func replaceMySQLTCPHostPort(raw, host string, port int) (string, error) {
	re := regexp.MustCompile(`@tcp\(([^)]+)\)`)
	if !re.MatchString(raw) {
		return "", errors.New("mysql connection string missing @tcp(host:port)")
	}
	return re.ReplaceAllString(raw, fmt.Sprintf("@tcp(%s:%d)", host, port)), nil
}

func sqlDSN(conn model.Connection) (dsn, driver string, err error) {
	switch conn.Type {
	case model.DBTypePostgres:
		driver = "pgx"
		if conn.UseConnString {
			return conn.ConnString, driver, nil
		}
		port := conn.Port
		if port == 0 {
			port = 5432
		}
		u := &url.URL{
			Scheme: "postgres",
			Host:   fmt.Sprintf("%s:%d", conn.Host, port),
			Path:   "/" + conn.Database,
		}
		if conn.Password != "" {
			u.User = url.UserPassword(conn.Username, conn.Password)
		} else {
			u.User = url.User(conn.Username)
		}
		q := u.Query()
		if q.Get("sslmode") == "" {
			q.Set("sslmode", "disable")
		}
		u.RawQuery = q.Encode()
		return u.String(), driver, nil

	case model.DBTypeMySQL:
		driver = "mysql"
		if conn.UseConnString {
			return conn.ConnString, driver, nil
		}
		port := conn.Port
		if port == 0 {
			port = 3306
		}
		dbName := conn.Database
		if dbName == "" {
			dbName = "mysql"
		}
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true", conn.Username, conn.Password, conn.Host, port, dbName), driver, nil
	default:
		return "", "", errors.New("not a SQL database type")
	}
}

func mongoURI(conn model.Connection) (string, error) {
	if conn.UseConnString {
		return conn.ConnString, nil
	}
	port := conn.Port
	if port == 0 {
		port = 27017
	}
	dbName := conn.Database
	if dbName == "" {
		dbName = "admin"
	}
	u := &url.URL{
		Scheme: "mongodb",
		Host:   fmt.Sprintf("%s:%d", conn.Host, port),
		Path:   "/" + dbName,
	}
	if conn.Username != "" {
		if conn.Password != "" {
			u.User = url.UserPassword(conn.Username, conn.Password)
		} else {
			u.User = url.User(conn.Username)
		}
	}
	return u.String(), nil
}

func readMongoDB(conn model.Connection) string {
	if conn.Database != "" {
		return conn.Database
	}
	if !conn.UseConnString {
		return "admin"
	}
	u, err := url.Parse(conn.ConnString)
	if err != nil {
		return "admin"
	}
	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		return "admin"
	}
	if idx := strings.Index(path, "?"); idx > 0 {
		return path[:idx]
	}
	return path
}

func redisOptions(conn model.Connection) (*redis.Options, error) {
	if conn.UseConnString {
		return redis.ParseURL(conn.ConnString)
	}
	port := conn.Port
	if port == 0 {
		port = 6379
	}
	opts := &redis.Options{
		Addr: fmt.Sprintf("%s:%d", conn.Host, port),
		DB:   0,
	}
	if conn.Username != "" {
		opts.Username = conn.Username
	}
	if conn.Password != "" {
		opts.Password = conn.Password
	}
	if conn.Database != "" {
		db, err := strconv.Atoi(conn.Database)
		if err != nil {
			return nil, fmt.Errorf("invalid redis DB index: %w", err)
		}
		opts.DB = db
	}
	return opts, nil
}

func defaultPort(dbType model.DBType) int {
	switch dbType {
	case model.DBTypePostgres:
		return 5432
	case model.DBTypeMySQL:
		return 3306
	case model.DBTypeMongo:
		return 27017
	case model.DBTypeRedis:
		return 6379
	default:
		return 0
	}
}
