package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	wailsruntime "github.com/wailsapp/wails/v2/pkg/runtime"
	"puppy-db-tool-desktop/internal/db"
	"puppy-db-tool-desktop/internal/model"
	"puppy-db-tool-desktop/internal/storage"
)

type App struct {
	ctx context.Context

	repo      *storage.Repository
	dbService *db.Service

	activeConnectionID string
	mu                 sync.RWMutex
	initErr            error
}

type ConnectionRow struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Type    string `json:"type"`
	Mode    string `json:"mode"`
	Status  string `json:"status"`
	Subline string `json:"subline"`
}

type ConnectionPage struct {
	Items      []ConnectionRow `json:"items"`
	Total      int             `json:"total"`
	Page       int             `json:"page"`
	PageSize   int             `json:"pageSize"`
	TotalPages int             `json:"totalPages"`
}

type SSHInput struct {
	Enabled    bool   `json:"enabled"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	AuthType   string `json:"authType"`
	Password   string `json:"password"`
	KeyFile    string `json:"keyFile"`
	Passphrase string `json:"passphrase"`
}

type ConnectionInput struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Type          string   `json:"type"`
	UseConnString bool     `json:"useConnString"`
	ConnString    string   `json:"connString"`
	Host          string   `json:"host"`
	Port          int      `json:"port"`
	Database      string   `json:"database"`
	Schema        string   `json:"schema"`
	Username      string   `json:"username"`
	Password      string   `json:"password"`
	SSH           SSHInput `json:"ssh"`
}

type ActiveConnectionInfo struct {
	HasActive  bool          `json:"hasActive"`
	Connection ConnectionRow `json:"connection"`
}

type TableDataRequest struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Filter string `json:"filter"`
	Sort   string `json:"sort"`
	Limit  int    `json:"limit"`
	Page   int    `json:"page"`
}

type TableDataResponse struct {
	Schema   string     `json:"schema"`
	Table    string     `json:"table"`
	Columns  []string   `json:"columns"`
	Rows     [][]string `json:"rows"`
	Page     int        `json:"page"`
	PageSize int        `json:"pageSize"`
	HasNext  bool       `json:"hasNext"`
	From     int        `json:"from"`
	To       int        `json:"to"`
}

type RowMutationRequest struct {
	Schema    string            `json:"schema"`
	Table     string            `json:"table"`
	KeyColumn string            `json:"keyColumn"`
	KeyValue  string            `json:"keyValue"`
	Row       map[string]string `json:"row"`
}

type DeleteRowRequest struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	KeyColumn string `json:"keyColumn"`
	KeyValue  string `json:"keyValue"`
}

type QueryRunResponse struct {
	Columns      []string   `json:"columns"`
	Rows         [][]string `json:"rows"`
	RowsAffected int64      `json:"rowsAffected"`
	Message      string     `json:"message"`
	DurationMs   int64      `json:"durationMs"`
}

type SnippetItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Connection string `json:"connection"`
	Type       string `json:"type"`
	Query      string `json:"query"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

type HistoryItem struct {
	ID           string `json:"id"`
	ConnectionID string `json:"connection_id"`
	Type         string `json:"type"`
	Query        string `json:"query"`
	DurationMs   int64  `json:"duration_ms"`
	Error        string `json:"error,omitempty"`
	ExecutedAt   string `json:"executed_at"`
}

func NewApp() *App {
	out := &App{dbService: db.NewService()}

	store, err := storage.NewStore()
	if err != nil {
		out.initErr = err
		return out
	}
	repo, err := storage.NewRepository(store)
	if err != nil {
		out.initErr = err
		return out
	}
	out.repo = repo
	return out
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

func (a *App) shutdown(ctx context.Context) {
	a.dbService.Close()
}

func (a *App) ready() error {
	if a.initErr != nil {
		return a.initErr
	}
	if a.repo == nil {
		return errors.New("repository is not initialized")
	}
	return nil
}

func (a *App) currentActiveID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.activeConnectionID
}

func (a *App) activeConnectionModel() (model.Connection, error) {
	id := a.currentActiveID()
	if id == "" {
		return model.Connection{}, errors.New("no active connection")
	}
	conn, err := a.repo.ConnectionByID(id)
	if err != nil {
		return model.Connection{}, err
	}
	return conn, nil
}

func (a *App) ListConnections(search string, page int, pageSize int) (ConnectionPage, error) {
	if err := a.ready(); err != nil {
		return ConnectionPage{}, err
	}
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 4
	}
	if pageSize > 100 {
		pageSize = 100
	}

	query := strings.ToLower(strings.TrimSpace(search))
	connections := a.repo.Connections()
	rows := make([]ConnectionRow, 0, len(connections))
	for _, conn := range connections {
		host, port := connectionHostPort(conn)
		row := a.connectionToRow(conn, host, port)
		if query != "" {
			bag := strings.ToLower(strings.Join([]string{
				row.Name,
				row.Host,
				row.Type,
				row.Mode,
				row.Subline,
				strconv.Itoa(row.Port),
			}, " "))
			if !strings.Contains(bag, query) {
				continue
			}
		}
		rows = append(rows, row)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return strings.ToLower(rows[i].Name) < strings.ToLower(rows[j].Name)
	})

	total := len(rows)
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * pageSize
	if start < 0 {
		start = 0
	}
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}

	return ConnectionPage{
		Items:      rows[start:end],
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

func (a *App) SaveConnection(input ConnectionInput) (ConnectionRow, error) {
	if err := a.ready(); err != nil {
		return ConnectionRow{}, err
	}

	t, err := parseDBType(input.Type)
	if err != nil {
		return ConnectionRow{}, err
	}

	name := strings.TrimSpace(input.Name)
	if name == "" {
		return ConnectionRow{}, errors.New("connection name is required")
	}
	if input.UseConnString {
		if strings.TrimSpace(input.ConnString) == "" {
			return ConnectionRow{}, errors.New("connection string is required")
		}
	} else {
		if strings.TrimSpace(input.Host) == "" {
			return ConnectionRow{}, errors.New("host is required")
		}
		if input.Port <= 0 {
			input.Port = defaultPort(t)
		}
	}

	sshCfg := model.SSHConfig{}
	if input.SSH.Enabled {
		sshCfg.Enabled = true
		sshCfg.Host = strings.TrimSpace(input.SSH.Host)
		sshCfg.Port = input.SSH.Port
		if sshCfg.Port == 0 {
			sshCfg.Port = 22
		}
		sshCfg.User = strings.TrimSpace(input.SSH.User)
		if sshCfg.Host == "" || sshCfg.User == "" {
			return ConnectionRow{}, errors.New("ssh host and user are required")
		}
		if strings.EqualFold(strings.TrimSpace(input.SSH.AuthType), "key_file") {
			sshCfg.AuthType = model.SSHAuthKeyFile
			sshCfg.KeyFile = strings.TrimSpace(input.SSH.KeyFile)
			sshCfg.Passphrase = input.SSH.Passphrase
			if sshCfg.KeyFile == "" {
				return ConnectionRow{}, errors.New("ssh key file is required")
			}
		} else {
			sshCfg.AuthType = model.SSHAuthPassword
			sshCfg.Password = input.SSH.Password
			if sshCfg.Password == "" {
				return ConnectionRow{}, errors.New("ssh password is required")
			}
		}
	}

	conn := model.Connection{
		ID:            strings.TrimSpace(input.ID),
		Name:          name,
		Type:          t,
		UseConnString: input.UseConnString,
		ConnString:    strings.TrimSpace(input.ConnString),
		Host:          strings.TrimSpace(input.Host),
		Port:          input.Port,
		Database:      strings.TrimSpace(input.Database),
		Schema:        strings.TrimSpace(input.Schema),
		Username:      strings.TrimSpace(input.Username),
		Password:      input.Password,
		SSH:           sshCfg,
	}

	saved, err := a.repo.UpsertConnection(conn)
	if err != nil {
		return ConnectionRow{}, err
	}
	host, port := connectionHostPort(saved)
	return a.connectionToRow(saved, host, port), nil
}

func (a *App) GetConnection(id string) (ConnectionInput, error) {
	if err := a.ready(); err != nil {
		return ConnectionInput{}, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return ConnectionInput{}, errors.New("id is required")
	}
	conn, err := a.repo.ConnectionByID(id)
	if err != nil {
		return ConnectionInput{}, err
	}
	return ConnectionInput{
		ID:            conn.ID,
		Name:          conn.Name,
		Type:          string(conn.Type),
		UseConnString: conn.UseConnString,
		ConnString:    conn.ConnString,
		Host:          conn.Host,
		Port:          conn.Port,
		Database:      conn.Database,
		Schema:        conn.Schema,
		Username:      conn.Username,
		Password:      conn.Password,
		SSH: SSHInput{
			Enabled:    conn.SSH.Enabled,
			Host:       conn.SSH.Host,
			Port:       conn.SSH.Port,
			User:       conn.SSH.User,
			AuthType:   string(conn.SSH.AuthType),
			Password:   conn.SSH.Password,
			KeyFile:    conn.SSH.KeyFile,
			Passphrase: conn.SSH.Passphrase,
		},
	}, nil
}

func (a *App) DeleteConnection(id string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("id is required")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.activeConnectionID == id {
		a.dbService.Disconnect(id)
		a.activeConnectionID = ""
	}
	return a.repo.DeleteConnection(id)
}

func (a *App) OpenConnection(id string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("id is required")
	}

	conn, err := a.repo.ConnectionByID(id)
	if err != nil {
		return err
	}
	if err := a.dbService.Connect(conn); err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeConnectionID != "" && a.activeConnectionID != id {
		a.dbService.Disconnect(a.activeConnectionID)
	}
	a.activeConnectionID = id
	return nil
}

func (a *App) DisconnectActive() error {
	if err := a.ready(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeConnectionID == "" {
		return nil
	}
	a.dbService.Disconnect(a.activeConnectionID)
	a.activeConnectionID = ""
	return nil
}

func (a *App) GetActiveConnection() (ActiveConnectionInfo, error) {
	if err := a.ready(); err != nil {
		return ActiveConnectionInfo{}, err
	}
	id := a.currentActiveID()
	if id == "" {
		return ActiveConnectionInfo{HasActive: false}, nil
	}
	conn, err := a.repo.ConnectionByID(id)
	if err != nil {
		return ActiveConnectionInfo{}, err
	}
	host, port := connectionHostPort(conn)
	return ActiveConnectionInfo{
		HasActive:  true,
		Connection: a.connectionToRow(conn, host, port),
	}, nil
}

func (a *App) TestConnection(input ConnectionInput) error {
	if err := a.ready(); err != nil {
		return err
	}

	t, err := parseDBType(input.Type)
	if err != nil {
		return err
	}
	conn := model.Connection{
		Name:          strings.TrimSpace(input.Name),
		Type:          t,
		UseConnString: input.UseConnString,
		ConnString:    strings.TrimSpace(input.ConnString),
		Host:          strings.TrimSpace(input.Host),
		Port:          input.Port,
		Database:      strings.TrimSpace(input.Database),
		Schema:        strings.TrimSpace(input.Schema),
		Username:      strings.TrimSpace(input.Username),
		Password:      input.Password,
		SSH: model.SSHConfig{
			Enabled:    input.SSH.Enabled,
			Host:       strings.TrimSpace(input.SSH.Host),
			Port:       input.SSH.Port,
			User:       strings.TrimSpace(input.SSH.User),
			AuthType:   model.SSHAuthType(strings.TrimSpace(input.SSH.AuthType)),
			Password:   input.SSH.Password,
			KeyFile:    strings.TrimSpace(input.SSH.KeyFile),
			Passphrase: input.SSH.Passphrase,
		},
	}
	if conn.Port == 0 {
		conn.Port = defaultPort(conn.Type)
	}
	if conn.SSH.Enabled && conn.SSH.Port == 0 {
		conn.SSH.Port = 22
	}
	if conn.SSH.Enabled && conn.SSH.AuthType == "" {
		conn.SSH.AuthType = model.SSHAuthPassword
	}
	return a.dbService.TestConnection(conn)
}

func (a *App) PickSSHKeyFile() (string, error) {
	if a.ctx == nil {
		return "", errors.New("app context is not ready")
	}
	path, err := wailsruntime.OpenFileDialog(a.ctx, wailsruntime.OpenDialogOptions{
		Title: "Select SSH Private Key",
		Filters: []wailsruntime.FileFilter{
			{
				DisplayName: "Private Key Files",
				Pattern:     "*.pem;*.ppk;*.key;id_rsa*",
			},
			{
				DisplayName: "All Files",
				Pattern:     "*",
			},
		},
	})
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(path), nil
}

func (a *App) SaveTextFile(defaultFilename, content string) (string, error) {
	if a.ctx == nil {
		return "", errors.New("app context is not ready")
	}
	path, err := wailsruntime.SaveFileDialog(a.ctx, wailsruntime.SaveDialogOptions{
		Title:           "Save File",
		DefaultFilename: strings.TrimSpace(defaultFilename),
	})
	if err != nil {
		return "", err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (a *App) ListObjectsActive() ([]model.DBObject, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	id := a.currentActiveID()
	if id == "" {
		return []model.DBObject{}, nil
	}
	return a.dbService.ListObjects(id)
}

func (a *App) ListSchemasActive() ([]string, error) {
	objects, err := a.ListObjectsActive()
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, obj := range objects {
		if obj.Type == "schema" || obj.Type == "database" {
			if strings.TrimSpace(obj.Name) != "" {
				set[obj.Name] = struct{}{}
			}
		}
		if strings.TrimSpace(obj.Schema) != "" {
			set[obj.Schema] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for schema := range set {
		out = append(out, schema)
	}
	sort.Strings(out)
	return out, nil
}

func (a *App) ListTableInfoActive(schema string) ([]model.TableInfo, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	id := a.currentActiveID()
	if id == "" {
		return []model.TableInfo{}, nil
	}
	return a.dbService.ListTableInfo(id, strings.TrimSpace(schema))
}

func (a *App) DescribeTableColumnsActive(schema, table string) ([]model.TableColumn, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	id := a.currentActiveID()
	if id == "" {
		return []model.TableColumn{}, nil
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return nil, errors.New("table is required")
	}
	return a.dbService.DescribeTableColumns(id, strings.TrimSpace(schema), table)
}

func (a *App) FetchTableDataActive(req TableDataRequest) (TableDataResponse, error) {
	if err := a.ready(); err != nil {
		return TableDataResponse{}, err
	}
	id := a.currentActiveID()
	if id == "" {
		return TableDataResponse{}, errors.New("no active connection")
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		return TableDataResponse{}, errors.New("table is required")
	}
	page := req.Page
	if page < 1 {
		page = 1
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := (page - 1) * limit
	fetchLimit := limit + 1

	data, err := a.dbService.FetchTableData(id, strings.TrimSpace(req.Schema), table, strings.TrimSpace(req.Filter), strings.TrimSpace(req.Sort), fetchLimit, offset)
	if err != nil {
		return TableDataResponse{}, err
	}

	hasNext := len(data.Rows) > limit
	rows := data.Rows
	if hasNext {
		rows = rows[:limit]
	}
	from := 0
	to := 0
	if len(rows) > 0 {
		from = offset + 1
		to = offset + len(rows)
	}
	return TableDataResponse{
		Schema:   strings.TrimSpace(req.Schema),
		Table:    table,
		Columns:  data.Columns,
		Rows:     rows,
		Page:     page,
		PageSize: limit,
		HasNext:  hasNext,
		From:     from,
		To:       to,
	}, nil
}

func (a *App) CountTableRowsActive(schema, table, filter string) (int64, error) {
	if err := a.ready(); err != nil {
		return 0, err
	}
	id := a.currentActiveID()
	if id == "" {
		return 0, errors.New("no active connection")
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return 0, errors.New("table is required")
	}
	return a.dbService.CountTableRows(id, strings.TrimSpace(schema), table, strings.TrimSpace(filter))
}

func (a *App) EmptyTableActive(schema, table string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}
	return a.dbService.EmptyTable(id, strings.TrimSpace(schema), table)
}

func (a *App) DropTableActive(schema, table string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}
	return a.dbService.DropTable(id, strings.TrimSpace(schema), table)
}

func (a *App) CreateTableActive(schema, table string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}
	return a.dbService.CreateTable(id, strings.TrimSpace(schema), table)
}

func (a *App) ExportDatabaseActive(schema string) (string, error) {
	if err := a.ready(); err != nil {
		return "", err
	}
	id := a.currentActiveID()
	if id == "" {
		return "", errors.New("no active connection")
	}

	content, err := a.dbService.ExportDatabase(id, strings.TrimSpace(schema))
	if err != nil {
		return "", err
	}

	conn, _ := a.repo.ConnectionByID(id)
	dbType, _ := a.dbService.ConnectionType(id)
	ext := "sql"
	if dbType == model.DBTypeMongo || dbType == model.DBTypeRedis {
		ext = "json"
	}
	nameParts := []string{
		safeFilenamePart(conn.Name),
		safeFilenamePart(strings.TrimSpace(schema)),
		time.Now().Format("20060102_150405"),
	}
	base := strings.Trim(strings.Join(nameParts, "_"), "_")
	if base == "" {
		base = "database_export_" + time.Now().Format("20060102_150405")
	}

	path, err := wailsruntime.SaveFileDialog(a.ctx, wailsruntime.SaveDialogOptions{
		Title:           "Export Database",
		DefaultFilename: base + "." + ext,
	})
	if err != nil {
		return "", err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (a *App) InsertRowActive(req RowMutationRequest) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	if strings.TrimSpace(req.Table) == "" {
		return errors.New("table is required")
	}
	return a.dbService.InsertRow(id, strings.TrimSpace(req.Schema), strings.TrimSpace(req.Table), req.Row)
}

func (a *App) UpdateRowActive(req RowMutationRequest) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	if strings.TrimSpace(req.Table) == "" {
		return errors.New("table is required")
	}
	if strings.TrimSpace(req.KeyColumn) == "" {
		return errors.New("key column is required")
	}
	return a.dbService.UpdateRow(id, strings.TrimSpace(req.Schema), strings.TrimSpace(req.Table), strings.TrimSpace(req.KeyColumn), req.KeyValue, req.Row)
}

func (a *App) DeleteRowActive(req DeleteRowRequest) error {
	if err := a.ready(); err != nil {
		return err
	}
	id := a.currentActiveID()
	if id == "" {
		return errors.New("no active connection")
	}
	if strings.TrimSpace(req.Table) == "" {
		return errors.New("table is required")
	}
	if strings.TrimSpace(req.KeyColumn) == "" {
		return errors.New("key column is required")
	}
	return a.dbService.DeleteRow(id, strings.TrimSpace(req.Schema), strings.TrimSpace(req.Table), strings.TrimSpace(req.KeyColumn), req.KeyValue)
}

func (a *App) RunQueryActive(query string) (QueryRunResponse, error) {
	if err := a.ready(); err != nil {
		return QueryRunResponse{}, err
	}
	id := a.currentActiveID()
	if id == "" {
		return QueryRunResponse{}, errors.New("no active connection")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return QueryRunResponse{}, errors.New("query is empty")
	}

	result, duration, err := a.dbService.RunQuery(id, query)

	historyItem := model.QueryHistory{
		ConnectionID: id,
		Query:        query,
		DurationMs:   duration.Milliseconds(),
		ExecutedAt:   time.Now(),
	}
	if conn, connErr := a.repo.ConnectionByID(id); connErr == nil {
		historyItem.Type = conn.Type
	}
	if err != nil {
		historyItem.Error = err.Error()
	}
	_ = a.repo.AppendHistory(historyItem)

	if err != nil {
		return QueryRunResponse{}, err
	}
	return QueryRunResponse{
		Columns:      result.Columns,
		Rows:         result.Rows,
		RowsAffected: result.RowsAffected,
		Message:      result.Message,
		DurationMs:   duration.Milliseconds(),
	}, nil
}

func (a *App) ListSnippets(activeOnly bool) ([]SnippetItem, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	items := a.repo.Snippets()
	filtered := make([]model.Snippet, 0, len(items))
	if !activeOnly {
		filtered = items
	} else {
		activeID := a.currentActiveID()
		if activeID == "" {
			return []SnippetItem{}, nil
		}
		for _, item := range items {
			if item.Connection == activeID {
				filtered = append(filtered, item)
			}
		}
	}

	out := make([]SnippetItem, 0, len(filtered))
	for _, item := range filtered {
		out = append(out, snippetToItem(item))
	}
	return out, nil
}

func (a *App) SaveSnippetActive(name, query string) (SnippetItem, error) {
	if err := a.ready(); err != nil {
		return SnippetItem{}, err
	}
	activeID := a.currentActiveID()
	if activeID == "" {
		return SnippetItem{}, errors.New("no active connection")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return SnippetItem{}, errors.New("query is empty")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "Snippet " + time.Now().Format("2006-01-02 15:04")
	}
	conn, err := a.repo.ConnectionByID(activeID)
	if err != nil {
		return SnippetItem{}, err
	}
	snippet := model.Snippet{
		Name:       name,
		Connection: activeID,
		Type:       conn.Type,
		Query:      query,
	}
	saved, err := a.repo.SaveSnippet(snippet)
	if err != nil {
		return SnippetItem{}, err
	}
	return snippetToItem(saved), nil
}

func (a *App) DeleteSnippet(id string) error {
	if err := a.ready(); err != nil {
		return err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("id is required")
	}
	return a.repo.DeleteSnippet(id)
}

func (a *App) ListHistory(activeOnly bool) ([]HistoryItem, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	items := a.repo.History()
	filtered := make([]model.QueryHistory, 0, len(items))
	if !activeOnly {
		filtered = items
	} else {
		activeID := a.currentActiveID()
		if activeID == "" {
			return []HistoryItem{}, nil
		}
		for _, item := range items {
			if item.ConnectionID == activeID {
				filtered = append(filtered, item)
			}
		}
	}

	out := make([]HistoryItem, 0, len(filtered))
	for _, item := range filtered {
		out = append(out, historyToItem(item))
	}
	return out, nil
}

func snippetToItem(snippet model.Snippet) SnippetItem {
	return SnippetItem{
		ID:         snippet.ID,
		Name:       snippet.Name,
		Connection: snippet.Connection,
		Type:       string(snippet.Type),
		Query:      snippet.Query,
		CreatedAt:  snippet.CreatedAt.Format(time.RFC3339),
		UpdatedAt:  snippet.UpdatedAt.Format(time.RFC3339),
	}
}

func historyToItem(item model.QueryHistory) HistoryItem {
	return HistoryItem{
		ID:           item.ID,
		ConnectionID: item.ConnectionID,
		Type:         string(item.Type),
		Query:        item.Query,
		DurationMs:   item.DurationMs,
		Error:        item.Error,
		ExecutedAt:   item.ExecutedAt.Format(time.RFC3339),
	}
}

func (a *App) connectionToRow(conn model.Connection, host string, port int) ConnectionRow {
	a.mu.RLock()
	active := a.activeConnectionID == conn.ID
	a.mu.RUnlock()

	mode := "DIRECT"
	if conn.SSH.Enabled {
		mode = "SSH TUNNEL"
	}
	status := "Inactive"
	if active {
		status = "Live"
	}
	subline := strings.ToUpper(string(conn.Type))
	if conn.Database != "" {
		subline = fmt.Sprintf("%s • %s", conn.Database, strings.ToUpper(string(conn.Type)))
	}
	return ConnectionRow{
		ID:      conn.ID,
		Name:    conn.Name,
		Host:    host,
		Port:    port,
		Type:    strings.ToUpper(string(conn.Type)),
		Mode:    mode,
		Status:  status,
		Subline: subline,
	}
}

func parseDBType(raw string) (model.DBType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "postgres", "postgresql":
		return model.DBTypePostgres, nil
	case "mysql":
		return model.DBTypeMySQL, nil
	case "mongo", "mongodb":
		return model.DBTypeMongo, nil
	case "redis":
		return model.DBTypeRedis, nil
	default:
		return "", fmt.Errorf("unsupported database type: %s", raw)
	}
}

func connectionHostPort(conn model.Connection) (string, int) {
	host := strings.TrimSpace(conn.Host)
	port := conn.Port

	if conn.UseConnString && (host == "" || port == 0) {
		switch conn.Type {
		case model.DBTypeMySQL:
			re := regexp.MustCompile(`@tcp\(([^)]+)\)`)
			match := re.FindStringSubmatch(conn.ConnString)
			if len(match) == 2 {
				tcpHost, tcpPort, ok := strings.Cut(match[1], ":")
				host = tcpHost
				if ok {
					if parsed, err := strconv.Atoi(tcpPort); err == nil {
						port = parsed
					}
				}
			}
		default:
			if u, err := url.Parse(conn.ConnString); err == nil {
				if u.Hostname() != "" {
					host = u.Hostname()
				}
				if parsed, err := strconv.Atoi(u.Port()); err == nil {
					port = parsed
				}
			}
		}
	}

	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = defaultPort(conn.Type)
	}
	return host, port
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

func safeFilenamePart(value string) string {
	clean := strings.TrimSpace(value)
	if clean == "" {
		return ""
	}
	clean = strings.ToLower(clean)
	clean = regexp.MustCompile(`[^a-z0-9._-]+`).ReplaceAllString(clean, "_")
	clean = strings.Trim(clean, "._-")
	return clean
}
