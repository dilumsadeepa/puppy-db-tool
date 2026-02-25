package model

import "time"

type DBType string

const (
	DBTypePostgres DBType = "postgres"
	DBTypeMySQL    DBType = "mysql"
	DBTypeMongo    DBType = "mongo"
	DBTypeRedis    DBType = "redis"
)

type SSHAuthType string

const (
	SSHAuthPassword SSHAuthType = "password"
	SSHAuthKeyFile  SSHAuthType = "key_file"
)

type SSHConfig struct {
	Enabled    bool        `json:"enabled"`
	Host       string      `json:"host"`
	Port       int         `json:"port"`
	User       string      `json:"user"`
	AuthType   SSHAuthType `json:"auth_type"`
	Password   string      `json:"password,omitempty"`
	KeyFile    string      `json:"key_file,omitempty"`
	Passphrase string      `json:"passphrase,omitempty"`
}

type Connection struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Type          DBType    `json:"type"`
	UseConnString bool      `json:"use_conn_string"`
	ConnString    string    `json:"conn_string,omitempty"`
	Host          string    `json:"host,omitempty"`
	Port          int       `json:"port,omitempty"`
	Database      string    `json:"database,omitempty"`
	Username      string    `json:"username,omitempty"`
	Password      string    `json:"password,omitempty"`
	Schema        string    `json:"schema,omitempty"`
	SSH           SSHConfig `json:"ssh"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Snippet struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Connection string    `json:"connection"`
	Type       DBType    `json:"type"`
	Query      string    `json:"query"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type QueryHistory struct {
	ID           string    `json:"id"`
	ConnectionID string    `json:"connection_id"`
	Type         DBType    `json:"type"`
	Query        string    `json:"query"`
	DurationMs   int64     `json:"duration_ms"`
	Error        string    `json:"error,omitempty"`
	ExecutedAt   time.Time `json:"executed_at"`
}

type AppData struct {
	Connections []Connection   `json:"connections"`
	Snippets    []Snippet      `json:"snippets"`
	History     []QueryHistory `json:"history"`
}

type DBObject struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

type TableInfo struct {
	Schema      string `json:"schema"`
	Name        string `json:"name"`
	Rows        int64  `json:"rows"`
	Size        string `json:"size"`
	LastUpdated string `json:"last_updated"`
}

type QueryResult struct {
	Columns      []string   `json:"columns"`
	Rows         [][]string `json:"rows"`
	RowsAffected int64      `json:"rows_affected"`
	Message      string     `json:"message"`
}

type TableData struct {
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
}

type TableColumn struct {
	Name         string   `json:"name"`
	DataType     string   `json:"dataType"`
	Nullable     bool     `json:"nullable"`
	DefaultValue string   `json:"defaultValue,omitempty"`
	IsPrimary    bool     `json:"isPrimary"`
	EnumValues   []string `json:"enumValues,omitempty"`
}
