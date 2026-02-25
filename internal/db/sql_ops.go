package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"puppy-db-tool-desktop/internal/model"
)

const dbNullSentinel = "__PUPPYDB_NULL__"

func (s *Service) runByType(sess *session, query string) (model.QueryResult, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return runSQLQuery(sess.sqlDB, query)
	case model.DBTypeMongo:
		return runMongoQuery(sess, query)
	case model.DBTypeRedis:
		return runRedisCommand(sess, query)
	default:
		return model.QueryResult{}, errors.New("unsupported database type")
	}
}

func runSQLQuery(db *sql.DB, query string) (model.QueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err == nil {
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			return model.QueryResult{}, err
		}

		resultRows, err := scanRows(rows, columns)
		if err != nil {
			return model.QueryResult{}, err
		}

		return model.QueryResult{
			Columns: columns,
			Rows:    resultRows,
			Message: fmt.Sprintf("%d row(s)", len(resultRows)),
		}, nil
	}

	execResult, execErr := db.ExecContext(ctx, query)
	if execErr != nil {
		return model.QueryResult{}, execErr
	}
	affected, _ := execResult.RowsAffected()
	return model.QueryResult{
		RowsAffected: affected,
		Message:      fmt.Sprintf("OK, %d row(s) affected", affected),
	}, nil
}

func (s *Service) listObjectsByType(sess *session) ([]model.DBObject, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres:
		return listPostgresObjects(sess.sqlDB)
	case model.DBTypeMySQL:
		return listMySQLObjects(sess.sqlDB)
	case model.DBTypeMongo:
		return listMongoObjects(sess)
	case model.DBTypeRedis:
		return listRedisObjects(sess)
	default:
		return nil, errors.New("unsupported database type")
	}
}

func (s *Service) listTableInfoByType(sess *session, schema string) ([]model.TableInfo, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres:
		return listPostgresTableInfo(sess.sqlDB, schema)
	case model.DBTypeMySQL:
		return listMySQLTableInfo(sess.sqlDB, schema)
	case model.DBTypeMongo:
		return listMongoTableInfo(sess, schema)
	case model.DBTypeRedis:
		return listRedisTableInfo(sess)
	default:
		return nil, errors.New("unsupported database type")
	}
}

func (s *Service) describeTableColumnsByType(sess *session, schema, table string) ([]model.TableColumn, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres:
		return describePostgresColumns(sess.sqlDB, schema, table)
	case model.DBTypeMySQL:
		return describeMySQLColumns(sess.sqlDB, schema, table)
	case model.DBTypeMongo:
		return describeMongoColumns(sess, table)
	case model.DBTypeRedis:
		return []model.TableColumn{
			{Name: "key", DataType: "string", Nullable: false, IsPrimary: true},
			{Name: "type", DataType: "string", Nullable: false},
			{Name: "value", DataType: "string", Nullable: true},
		}, nil
	default:
		return nil, errors.New("unsupported database type")
	}
}

func (s *Service) fetchTableDataByType(sess *session, schema, table, filter, sortClause string, limit, offset int) (model.TableData, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return fetchSQLTableData(sess, schema, table, filter, sortClause, limit, offset)
	case model.DBTypeMongo:
		return fetchMongoTableData(sess, table, filter, sortClause, limit, offset)
	case model.DBTypeRedis:
		return fetchRedisData(sess, table, limit)
	default:
		return model.TableData{}, errors.New("unsupported database type")
	}
}

func (s *Service) countRowsByType(sess *session, schema, table, filter string) (int64, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return countSQLRows(sess, schema, table, filter)
	case model.DBTypeMongo:
		return countMongoRows(sess, table, filter)
	case model.DBTypeRedis:
		return countRedisRows(sess, table)
	default:
		return 0, errors.New("unsupported database type")
	}
}

func (s *Service) insertByType(sess *session, schema, table string, row map[string]string) error {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return insertSQLRow(sess, schema, table, row)
	case model.DBTypeMongo:
		return insertMongoRow(sess, table, row)
	case model.DBTypeRedis:
		return insertRedisValue(sess, row)
	default:
		return errors.New("unsupported database type")
	}
}

func (s *Service) updateByType(sess *session, schema, table, keyColumn, keyValue string, row map[string]string) error {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return updateSQLRow(sess, schema, table, keyColumn, keyValue, row)
	case model.DBTypeMongo:
		return updateMongoRow(sess, table, keyColumn, keyValue, row)
	case model.DBTypeRedis:
		return updateRedisValue(sess, keyValue, row)
	default:
		return errors.New("unsupported database type")
	}
}

func (s *Service) deleteByType(sess *session, schema, table, keyColumn, keyValue string) error {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return deleteSQLRow(sess, schema, table, keyColumn, keyValue)
	case model.DBTypeMongo:
		return deleteMongoRow(sess, table, keyColumn, keyValue)
	case model.DBTypeRedis:
		return deleteRedisValue(sess, keyValue)
	default:
		return errors.New("unsupported database type")
	}
}

func (s *Service) emptyTableByType(sess *session, schema, table string) error {
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}

	switch sess.conn.Type {
	case model.DBTypePostgres:
		query := "TRUNCATE TABLE " + qualifiedTable(sess.conn.Type, strings.TrimSpace(schema), table)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := sess.sqlDB.ExecContext(ctx, query)
		return err
	case model.DBTypeMySQL:
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		query := "TRUNCATE TABLE " + qualifiedTable(sess.conn.Type, strings.TrimSpace(schema), table)
		return withMySQLForeignKeyChecksDisabled(ctx, sess.sqlDB, func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, query)
			return err
		})
	case model.DBTypeMongo:
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := sess.mongoClient.Database(sess.mongoDB).Collection(table).DeleteMany(ctx, bson.M{})
		return err
	case model.DBTypeRedis:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return sess.redisClient.Del(ctx, table).Err()
	default:
		return errors.New("unsupported database type")
	}
}

func (s *Service) dropTableByType(sess *session, schema, table string) error {
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}

	switch sess.conn.Type {
	case model.DBTypePostgres:
		query := "DROP TABLE IF EXISTS " + qualifiedTable(sess.conn.Type, strings.TrimSpace(schema), table)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := sess.sqlDB.ExecContext(ctx, query)
		return err
	case model.DBTypeMySQL:
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		query := "DROP TABLE IF EXISTS " + qualifiedTable(sess.conn.Type, strings.TrimSpace(schema), table)
		return withMySQLForeignKeyChecksDisabled(ctx, sess.sqlDB, func(conn *sql.Conn) error {
			_, err := conn.ExecContext(ctx, query)
			return err
		})
	case model.DBTypeMongo:
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return sess.mongoClient.Database(sess.mongoDB).Collection(table).Drop(ctx)
	case model.DBTypeRedis:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return sess.redisClient.Del(ctx, table).Err()
	default:
		return errors.New("unsupported database type")
	}
}

func withMySQLForeignKeyChecksDisabled(ctx context.Context, db *sql.DB, run func(conn *sql.Conn) error) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return err
	}
	defer func() {
		if _, resetErr := conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); resetErr != nil && err == nil {
			err = resetErr
		}
	}()

	return run(conn)
}

func (s *Service) createTableByType(sess *session, schema, table string) error {
	table = strings.TrimSpace(table)
	if table == "" {
		return errors.New("table is required")
	}

	switch sess.conn.Type {
	case model.DBTypePostgres:
		schema = strings.TrimSpace(schema)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if schema != "" {
			if _, err := sess.sqlDB.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdent(model.DBTypePostgres, schema)); err != nil {
				return err
			}
		}
		query := fmt.Sprintf(
			"CREATE TABLE %s (id BIGSERIAL PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
			qualifiedTable(model.DBTypePostgres, schema, table),
		)
		_, err := sess.sqlDB.ExecContext(ctx, query)
		return err
	case model.DBTypeMySQL:
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		query := fmt.Sprintf(
			"CREATE TABLE %s (id BIGINT AUTO_INCREMENT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			qualifiedTable(model.DBTypeMySQL, strings.TrimSpace(schema), table),
		)
		_, err := sess.sqlDB.ExecContext(ctx, query)
		return err
	case model.DBTypeMongo:
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return sess.mongoClient.Database(sess.mongoDB).CreateCollection(ctx, table)
	case model.DBTypeRedis:
		return errors.New("create table is not supported for redis")
	default:
		return errors.New("unsupported database type")
	}
}

func (s *Service) exportDatabaseByType(sess *session, schema string) (string, error) {
	switch sess.conn.Type {
	case model.DBTypePostgres, model.DBTypeMySQL:
		return exportSQLDatabase(sess, schema)
	case model.DBTypeMongo:
		return exportMongoDatabase(sess, schema)
	case model.DBTypeRedis:
		return exportRedisDatabase(sess)
	default:
		return "", errors.New("unsupported database type")
	}
}

func exportSQLDatabase(sess *session, schema string) (string, error) {
	tables, normalizedSchema, err := listSQLExportTables(sess, schema)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	out.WriteString("-- Puppy DB Tool export\n")
	out.WriteString("-- Type: " + string(sess.conn.Type) + "\n")
	out.WriteString("-- Schema: " + normalizedSchema + "\n")
	out.WriteString("-- Generated: " + time.Now().Format(time.RFC3339) + "\n\n")

	if len(tables) == 0 {
		out.WriteString("-- No tables found.\n")
		return out.String(), nil
	}

	for _, table := range tables {
		ddl, err := exportTableDDL(sess, normalizedSchema, table)
		if err != nil {
			return "", err
		}
		dataSQL, err := exportTableDataSQL(sess, normalizedSchema, table)
		if err != nil {
			return "", err
		}

		out.WriteString("-- ------------------------------------------------------------------\n")
		out.WriteString("-- Table: " + table + "\n")
		out.WriteString("-- ------------------------------------------------------------------\n")
		out.WriteString("DROP TABLE IF EXISTS " + qualifiedTable(sess.conn.Type, normalizedSchema, table) + ";\n")
		out.WriteString(ddl)
		if !strings.HasSuffix(strings.TrimSpace(ddl), ";") {
			out.WriteString(";")
		}
		out.WriteString("\n\n")
		if strings.TrimSpace(dataSQL) != "" {
			out.WriteString(dataSQL)
			out.WriteString("\n")
		}
	}

	return out.String(), nil
}

func listSQLExportTables(sess *session, schema string) ([]string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	normalized := strings.TrimSpace(schema)
	if sess.conn.Type == model.DBTypePostgres {
		if normalized == "" {
			normalized = "public"
		}
		rows, err := sess.sqlDB.QueryContext(ctx, `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = $1 AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`, normalized)
		if err != nil {
			return nil, "", err
		}
		defer rows.Close()

		tables := make([]string, 0, 32)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, "", err
			}
			tables = append(tables, name)
		}
		return tables, normalized, rows.Err()
	}

	if normalized == "" {
		var dbName sql.NullString
		if err := sess.sqlDB.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&dbName); err != nil {
			return nil, "", err
		}
		if !dbName.Valid || strings.TrimSpace(dbName.String) == "" {
			return nil, "", errors.New("schema/database is required")
		}
		normalized = dbName.String
	}

	rows, err := sess.sqlDB.QueryContext(ctx, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = ? AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`, normalized)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	tables := make([]string, 0, 32)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, "", err
		}
		tables = append(tables, name)
	}
	return tables, normalized, rows.Err()
}

func exportTableDDL(sess *session, schema, table string) (string, error) {
	switch sess.conn.Type {
	case model.DBTypeMySQL:
		query := "SHOW CREATE TABLE " + qualifiedTable(model.DBTypeMySQL, schema, table)
		var tableName, createSQL string
		if err := sess.sqlDB.QueryRow(query).Scan(&tableName, &createSQL); err != nil {
			return "", err
		}
		return createSQL + ";", nil
	case model.DBTypePostgres:
		columns, err := describePostgresColumns(sess.sqlDB, schema, table)
		if err != nil {
			return "", err
		}
		lines := make([]string, 0, len(columns)+1)
		pkCols := make([]string, 0, 2)
		for _, column := range columns {
			colType := strings.TrimSpace(column.DataType)
			if colType == "" {
				colType = "text"
			}
			line := fmt.Sprintf("  %s %s", quoteIdent(model.DBTypePostgres, column.Name), colType)
			if column.DefaultValue != "" {
				line += " DEFAULT " + column.DefaultValue
			}
			if !column.Nullable {
				line += " NOT NULL"
			}
			lines = append(lines, line)
			if column.IsPrimary {
				pkCols = append(pkCols, quoteIdent(model.DBTypePostgres, column.Name))
			}
		}
		if len(pkCols) > 0 {
			lines = append(lines, "  PRIMARY KEY ("+strings.Join(pkCols, ", ")+")")
		}
		return "CREATE TABLE " + qualifiedTable(model.DBTypePostgres, schema, table) + " (\n" + strings.Join(lines, ",\n") + "\n);", nil
	default:
		return "", errors.New("ddl export is only supported for sql databases")
	}
}

func exportTableDataSQL(sess *session, schema, table string) (string, error) {
	query := "SELECT * FROM " + qualifiedTable(sess.conn.Type, schema, table)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rows, err := sess.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", nil
	}

	var out strings.Builder
	insertPrefix := "INSERT INTO " + qualifiedTable(sess.conn.Type, schema, table) + " (" + quotedColumns(sess.conn.Type, columns) + ") VALUES "
	rowCount := 0
	for rows.Next() {
		values := make([]any, len(columns))
		scans := make([]any, len(columns))
		for i := range values {
			scans[i] = &values[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return "", err
		}

		literals := make([]string, len(columns))
		for i, value := range values {
			literals[i] = exportSQLLiteral(value)
		}
		out.WriteString(insertPrefix)
		out.WriteString("(")
		out.WriteString(strings.Join(literals, ", "))
		out.WriteString(");\n")
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if rowCount == 0 {
		return "-- No rows in " + table + ".\n", nil
	}
	return out.String(), nil
}

func exportSQLLiteral(v any) string {
	if v == nil {
		return "NULL"
	}
	switch t := v.(type) {
	case bool:
		if t {
			return "TRUE"
		}
		return "FALSE"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", t)
	case time.Time:
		return "'" + escapeSQLString(t.Format("2006-01-02 15:04:05")) + "'"
	case []byte:
		return "'" + escapeSQLString(string(t)) + "'"
	default:
		return "'" + escapeSQLString(fmt.Sprintf("%v", t)) + "'"
	}
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func exportMongoDatabase(sess *session, schema string) (string, error) {
	dbName := strings.TrimSpace(schema)
	if dbName == "" {
		dbName = sess.mongoDB
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	names, err := sess.mongoClient.Database(dbName).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return "", err
	}

	payload := map[string]any{
		"type":        "mongo",
		"database":    dbName,
		"generatedAt": time.Now().Format(time.RFC3339),
		"collections": map[string]any{},
	}

	collections := map[string]any{}
	for _, name := range names {
		cursor, err := sess.mongoClient.Database(dbName).Collection(name).Find(ctx, bson.M{})
		if err != nil {
			return "", err
		}
		docs := []bson.M{}
		if err := cursor.All(ctx, &docs); err != nil {
			return "", err
		}
		collections[name] = docs
	}
	payload["collections"] = collections

	blob, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}
	return string(blob), nil
}

func exportRedisDatabase(sess *session) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	keys, err := sess.redisClient.Keys(ctx, "*").Result()
	if err != nil {
		return "", err
	}
	sort.Strings(keys)

	data := map[string]any{}
	for _, key := range keys {
		keyType, _ := sess.redisClient.Type(ctx, key).Result()
		entry := map[string]any{
			"type": keyType,
		}
		switch keyType {
		case "string":
			value, _ := sess.redisClient.Get(ctx, key).Result()
			entry["value"] = value
		case "hash":
			value, _ := sess.redisClient.HGetAll(ctx, key).Result()
			entry["value"] = value
		case "list":
			value, _ := sess.redisClient.LRange(ctx, key, 0, -1).Result()
			entry["value"] = value
		case "set":
			value, _ := sess.redisClient.SMembers(ctx, key).Result()
			sort.Strings(value)
			entry["value"] = value
		case "zset":
			value, _ := sess.redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
			entry["value"] = value
		default:
			entry["value"] = "(unsupported type export)"
		}
		data[key] = entry
	}

	payload := map[string]any{
		"type":        "redis",
		"generatedAt": time.Now().Format(time.RFC3339),
		"keys":        data,
	}
	blob, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}
	return string(blob), nil
}

func listPostgresObjects(db *sql.DB) ([]model.DBObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	objects := []model.DBObject{}

	schemaRows, err := db.QueryContext(ctx, `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schema_name
	`)
	if err != nil {
		return nil, err
	}
	for schemaRows.Next() {
		var schema string
		if err := schemaRows.Scan(&schema); err != nil {
			schemaRows.Close()
			return nil, err
		}
		objects = append(objects, model.DBObject{Schema: schema, Name: schema, Type: "schema"})
	}
	_ = schemaRows.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name, table_type
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var schema, name, t string
		if err := rows.Scan(&schema, &name, &t); err != nil {
			rows.Close()
			return nil, err
		}
		kind := "table"
		if strings.EqualFold(t, "VIEW") {
			kind = "view"
		}
		objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: kind})
	}
	_ = rows.Close()

	routineRows, err := db.QueryContext(ctx, `
		SELECT routine_schema, routine_name, routine_type
		FROM information_schema.routines
		WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY routine_schema, routine_name
	`)
	if err == nil {
		for routineRows.Next() {
			var schema, name, routineType string
			if err := routineRows.Scan(&schema, &name, &routineType); err != nil {
				routineRows.Close()
				return nil, err
			}
			kind := "procedure"
			if strings.EqualFold(routineType, "FUNCTION") {
				kind = "function"
			}
			objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: kind})
		}
		_ = routineRows.Close()
	}

	triggerRows, err := db.QueryContext(ctx, `
		SELECT trigger_schema, trigger_name
		FROM information_schema.triggers
		WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY trigger_schema, trigger_name
	`)
	if err == nil {
		for triggerRows.Next() {
			var schema, name string
			if err := triggerRows.Scan(&schema, &name); err != nil {
				triggerRows.Close()
				return nil, err
			}
			objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: "trigger"})
		}
		_ = triggerRows.Close()
	}

	return objects, nil
}

func listMySQLObjects(db *sql.DB) ([]model.DBObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	schemas, err := listMySQLUserSchemas(ctx, db)
	if err != nil {
		return nil, err
	}

	objects := make([]model.DBObject, 0, len(schemas)*8)
	for _, schema := range schemas {
		objects = append(objects, model.DBObject{Schema: schema, Name: schema, Type: "schema"})
	}

	for _, schema := range schemas {
		tableRows, err := db.QueryContext(ctx, `
			SELECT table_name, table_type
			FROM information_schema.tables
			WHERE table_schema = ?
			ORDER BY table_name
		`, schema)
		if err == nil {
			for tableRows.Next() {
				var name, t string
				if err := tableRows.Scan(&name, &t); err != nil {
					tableRows.Close()
					return nil, err
				}
				kind := "table"
				if strings.EqualFold(t, "VIEW") {
					kind = "view"
				}
				objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: kind})
			}
			_ = tableRows.Close()
		} else {
			fallbackQuery := fmt.Sprintf("SHOW FULL TABLES FROM %s", quoteIdent(model.DBTypeMySQL, schema))
			fallbackRows, fallbackErr := db.QueryContext(ctx, fallbackQuery)
			if fallbackErr == nil {
				for fallbackRows.Next() {
					var name, t string
					if err := fallbackRows.Scan(&name, &t); err != nil {
						continue
					}
					kind := "table"
					if strings.EqualFold(t, "VIEW") {
						kind = "view"
					}
					objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: kind})
				}
				_ = fallbackRows.Close()
			}
		}

		procRows, err := db.QueryContext(ctx, `
			SELECT routine_name, routine_type
			FROM information_schema.routines
			WHERE routine_schema = ?
			ORDER BY routine_name
		`, schema)
		if err == nil {
			for procRows.Next() {
				var name, routineType string
				if err := procRows.Scan(&name, &routineType); err != nil {
					procRows.Close()
					return nil, err
				}
				kind := "procedure"
				if strings.EqualFold(routineType, "FUNCTION") {
					kind = "function"
				}
				objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: kind})
			}
			_ = procRows.Close()
		}

		triggerRows, err := db.QueryContext(ctx, `
			SELECT trigger_name
			FROM information_schema.triggers
			WHERE trigger_schema = ?
			ORDER BY trigger_name
		`, schema)
		if err == nil {
			for triggerRows.Next() {
				var name string
				if err := triggerRows.Scan(&name); err != nil {
					triggerRows.Close()
					return nil, err
				}
				objects = append(objects, model.DBObject{Schema: schema, Name: name, Type: "trigger"})
			}
			_ = triggerRows.Close()
		}
	}

	return objects, nil
}

func listPostgresTableInfo(db *sql.DB, schema string) ([]model.TableInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
		SELECT
			t.table_schema,
			t.table_name,
			COALESCE(s.n_live_tup, 0) AS row_count,
			pg_size_pretty(pg_total_relation_size((quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass)) AS size,
			COALESCE(to_char(GREATEST(s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze), 'YYYY-MM-DD HH24:MI:SS'), 'unknown') AS last_updated
		FROM information_schema.tables t
		LEFT JOIN pg_stat_user_tables s
			ON s.schemaname = t.table_schema AND s.relname = t.table_name
		WHERE t.table_type = 'BASE TABLE'
			AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
			AND ($1 = '' OR t.table_schema = $1)
		ORDER BY t.table_schema, t.table_name
	`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []model.TableInfo{}
	for rows.Next() {
		var item model.TableInfo
		if err := rows.Scan(&item.Schema, &item.Name, &item.Rows, &item.Size, &item.LastUpdated); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func listMySQLTableInfo(db *sql.DB, schema string) ([]model.TableInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if schema == "" {
		var current sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&current); err == nil && current.Valid {
			schema = current.String
		}
		if schema == "" {
			schemas, err := listMySQLUserSchemas(ctx, db)
			if err != nil {
				return nil, err
			}
			if len(schemas) == 0 {
				return []model.TableInfo{}, nil
			}
			schema = schemas[0]
		}
	}

	rows, err := db.QueryContext(ctx, `
		SELECT
			table_schema,
			table_name,
			COALESCE(table_rows, 0) AS row_count,
			CONCAT(ROUND((data_length + index_length) / 1024 / 1024, 2), ' MB') AS size,
			COALESCE(DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s'), 'unknown') AS last_updated
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
			AND table_schema = ?
		ORDER BY table_name
	`, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []model.TableInfo{}
	for rows.Next() {
		var item model.TableInfo
		if err := rows.Scan(&item.Schema, &item.Name, &item.Rows, &item.Size, &item.LastUpdated); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func describePostgresColumns(db *sql.DB, schema, table string) ([]model.TableColumn, error) {
	if strings.TrimSpace(table) == "" {
		return nil, errors.New("table is required")
	}
	if strings.TrimSpace(schema) == "" {
		schema = "public"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
		SELECT
			c.column_name,
			c.data_type,
			c.udt_name,
			c.is_nullable,
			c.column_default,
			COALESCE(tc.constraint_type = 'PRIMARY KEY', false) AS is_primary
		FROM information_schema.columns c
		LEFT JOIN information_schema.key_column_usage kcu
			ON c.table_schema = kcu.table_schema
			AND c.table_name = kcu.table_name
			AND c.column_name = kcu.column_name
		LEFT JOIN information_schema.table_constraints tc
			ON kcu.constraint_name = tc.constraint_name
			AND kcu.table_schema = tc.table_schema
			AND kcu.table_name = tc.table_name
			AND tc.constraint_type = 'PRIMARY KEY'
		WHERE c.table_schema = $1
			AND c.table_name = $2
		ORDER BY c.ordinal_position
	`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]model.TableColumn, 0, 32)
	for rows.Next() {
		var name, dataType, udtName, isNullable string
		var defaultValue sql.NullString
		var isPrimary bool
		if err := rows.Scan(&name, &dataType, &udtName, &isNullable, &defaultValue, &isPrimary); err != nil {
			return nil, err
		}

		cleanType := strings.ToLower(strings.TrimSpace(dataType))
		enumValues := []string{}
		if strings.EqualFold(cleanType, "user-defined") {
			cleanType = strings.ToLower(strings.TrimSpace(udtName))
			if vals, enumErr := listPostgresEnumValues(ctx, db, schema, udtName); enumErr == nil && len(vals) > 0 {
				enumValues = vals
			}
		}

		item := model.TableColumn{
			Name:       name,
			DataType:   cleanType,
			Nullable:   strings.EqualFold(isNullable, "YES"),
			IsPrimary:  isPrimary,
			EnumValues: enumValues,
		}
		if defaultValue.Valid {
			item.DefaultValue = defaultValue.String
		}
		columns = append(columns, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func listPostgresEnumValues(ctx context.Context, db *sql.DB, schema, udtName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = $1 AND t.typname = $2
		ORDER BY e.enumsortorder
	`, schema, udtName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make([]string, 0, 8)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, rows.Err()
}

func describeMySQLColumns(db *sql.DB, schema, table string) ([]model.TableColumn, error) {
	if strings.TrimSpace(table) == "" {
		return nil, errors.New("table is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	schema = strings.TrimSpace(schema)
	if schema == "" {
		var current sql.NullString
		if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&current); err == nil && current.Valid {
			schema = current.String
		}
	}
	if schema == "" {
		return nil, errors.New("schema/database is required for mysql")
	}

	rows, err := db.QueryContext(ctx, `
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			column_key,
			column_type
		FROM information_schema.columns
		WHERE table_schema = ?
			AND table_name = ?
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]model.TableColumn, 0, 32)
	for rows.Next() {
		var name, dataType, isNullable, columnKey string
		var defaultValue, columnType sql.NullString
		if err := rows.Scan(&name, &dataType, &isNullable, &defaultValue, &columnKey, &columnType); err != nil {
			return nil, err
		}
		item := model.TableColumn{
			Name:      name,
			DataType:  strings.ToLower(strings.TrimSpace(dataType)),
			Nullable:  strings.EqualFold(isNullable, "YES"),
			IsPrimary: strings.EqualFold(columnKey, "PRI"),
		}
		if defaultValue.Valid {
			item.DefaultValue = defaultValue.String
		}
		if columnType.Valid {
			item.EnumValues = parseMySQLEnumValues(columnType.String)
		}
		columns = append(columns, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func parseMySQLEnumValues(columnType string) []string {
	clean := strings.TrimSpace(columnType)
	if clean == "" {
		return nil
	}
	lower := strings.ToLower(clean)
	if !strings.HasPrefix(lower, "enum(") || !strings.HasSuffix(lower, ")") {
		return nil
	}
	re := regexp.MustCompile(`'((?:[^'\\]|\\.)*)'`)
	matches := re.FindAllStringSubmatch(clean, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		v := strings.ReplaceAll(match[1], `\'`, `'`)
		v = strings.ReplaceAll(v, `\\`, `\`)
		values = append(values, v)
	}
	return values
}

func listMySQLUserSchemas(ctx context.Context, db *sql.DB) ([]string, error) {
	schemaSet := map[string]struct{}{}

	rows, err := db.QueryContext(ctx, `
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY schema_name
	`)
	if err == nil {
		for rows.Next() {
			var schema string
			if err := rows.Scan(&schema); err != nil {
				rows.Close()
				return nil, err
			}
			if !isSystemMySQLSchema(schema) {
				schemaSet[schema] = struct{}{}
			}
		}
		_ = rows.Close()
	}

	if len(schemaSet) == 0 {
		showRows, showErr := db.QueryContext(ctx, `SHOW DATABASES`)
		if showErr != nil {
			if err != nil {
				// Keep going with additional fallbacks instead of failing early.
			} else {
				return nil, showErr
			}
		} else {
			for showRows.Next() {
				var schema string
				if scanErr := showRows.Scan(&schema); scanErr != nil {
					showRows.Close()
					return nil, scanErr
				}
				if !isSystemMySQLSchema(schema) {
					schemaSet[schema] = struct{}{}
				}
			}
			_ = showRows.Close()
		}
	}

	// Fallback to active schema if discovery is restricted.
	if len(schemaSet) == 0 {
		var current sql.NullString
		if currentErr := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&current); currentErr == nil && current.Valid {
			if !isSystemMySQLSchema(current.String) {
				schemaSet[current.String] = struct{}{}
			}
		}
	}

	// Final fallback from visible table metadata.
	if len(schemaSet) == 0 {
		tableSchemaRows, tableErr := db.QueryContext(ctx, `
			SELECT DISTINCT table_schema
			FROM information_schema.tables
			WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
			ORDER BY table_schema
		`)
		if tableErr == nil {
			for tableSchemaRows.Next() {
				var schema string
				if scanErr := tableSchemaRows.Scan(&schema); scanErr != nil {
					tableSchemaRows.Close()
					return nil, scanErr
				}
				if !isSystemMySQLSchema(schema) {
					schemaSet[schema] = struct{}{}
				}
			}
			_ = tableSchemaRows.Close()
		}
	}

	schemas := make([]string, 0, len(schemaSet))
	for schema := range schemaSet {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)
	return schemas, nil
}

func isSystemMySQLSchema(schema string) bool {
	switch strings.ToLower(strings.TrimSpace(schema)) {
	case "information_schema", "mysql", "performance_schema", "sys":
		return true
	default:
		return false
	}
}

func fetchSQLTableData(sess *session, schema, table, filter, sortClause string, limit, offset int) (model.TableData, error) {
	if table == "" {
		return model.TableData{}, errors.New("table is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	qTable := qualifiedTable(sess.conn.Type, schema, table)
	query := "SELECT * FROM " + qTable
	if strings.TrimSpace(filter) != "" {
		query += " WHERE " + filter
	}
	if strings.TrimSpace(sortClause) != "" {
		query += " ORDER BY " + sortClause
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := sess.sqlDB.QueryContext(ctx, query)
	if err != nil {
		return model.TableData{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return model.TableData{}, err
	}
	resultRows, err := scanRows(rows, columns)
	if err != nil {
		return model.TableData{}, err
	}

	return model.TableData{Columns: columns, Rows: resultRows}, nil
}

func countSQLRows(sess *session, schema, table, filter string) (int64, error) {
	if table == "" {
		return 0, errors.New("table is required")
	}
	query := "SELECT COUNT(*) FROM " + qualifiedTable(sess.conn.Type, schema, table)
	if strings.TrimSpace(filter) != "" {
		query += " WHERE " + filter
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var count int64
	if err := sess.sqlDB.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func insertSQLRow(sess *session, schema, table string, row map[string]string) error {
	if len(row) == 0 {
		return errors.New("insert row is empty")
	}
	keys := sortedKeys(row)
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, key := range keys {
		args[i] = inputToDBValue(row[key])
		placeholders[i] = placeholder(sess.conn.Type, i+1)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		qualifiedTable(sess.conn.Type, schema, table),
		quotedColumns(sess.conn.Type, keys),
		strings.Join(placeholders, ", "),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := sess.sqlDB.ExecContext(ctx, query, args...)
	return err
}

func updateSQLRow(sess *session, schema, table, keyColumn, keyValue string, row map[string]string) error {
	if keyColumn == "" {
		return errors.New("key column is required")
	}
	if len(row) == 0 {
		return errors.New("update row is empty")
	}

	keys := sortedKeys(row)
	sets := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)+1)
	argIndex := 1
	for _, key := range keys {
		if key == keyColumn {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = %s", quoteIdent(sess.conn.Type, key), placeholder(sess.conn.Type, argIndex)))
		args = append(args, inputToDBValue(row[key]))
		argIndex++
	}
	if len(sets) == 0 {
		return errors.New("no updatable fields provided")
	}
	args = append(args, keyValue)

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s = %s",
		qualifiedTable(sess.conn.Type, schema, table),
		strings.Join(sets, ", "),
		quoteIdent(sess.conn.Type, keyColumn),
		placeholder(sess.conn.Type, argIndex),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := sess.sqlDB.ExecContext(ctx, query, args...)
	return err
}

func deleteSQLRow(sess *session, schema, table, keyColumn, keyValue string) error {
	if keyColumn == "" {
		return errors.New("key column is required")
	}
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s = %s",
		qualifiedTable(sess.conn.Type, schema, table),
		quoteIdent(sess.conn.Type, keyColumn),
		placeholder(sess.conn.Type, 1),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := sess.sqlDB.ExecContext(ctx, query, keyValue)
	return err
}

func runMongoQuery(sess *session, query string) (model.QueryResult, error) {
	var command bson.M
	if err := bson.UnmarshalExtJSON([]byte(query), true, &command); err != nil {
		return model.QueryResult{}, fmt.Errorf("mongo expects JSON command, example: {\"ping\":1}: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res := sess.mongoClient.Database(sess.mongoDB).RunCommand(ctx, command)
	if err := res.Err(); err != nil {
		return model.QueryResult{}, err
	}
	var out bson.M
	if err := res.Decode(&out); err != nil {
		return model.QueryResult{}, err
	}
	blob, _ := json.MarshalIndent(out, "", "  ")
	return model.QueryResult{
		Columns: []string{"result"},
		Rows:    [][]string{{string(blob)}},
		Message: "Mongo command executed",
	}, nil
}

func fetchMongoTableData(sess *session, table, filter, sortClause string, limit, offset int) (model.TableData, error) {
	if table == "" {
		return model.TableData{}, errors.New("collection is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	criteria := bson.M{}
	if strings.TrimSpace(filter) != "" {
		if err := bson.UnmarshalExtJSON([]byte(filter), true, &criteria); err != nil {
			return model.TableData{}, fmt.Errorf("invalid mongo filter JSON: %w", err)
		}
	}

	findOptions := options.Find().SetLimit(int64(limit)).SetSkip(int64(offset))
	if strings.TrimSpace(sortClause) != "" {
		var sortDoc bson.M
		if err := bson.UnmarshalExtJSON([]byte(sortClause), true, &sortDoc); err != nil {
			return model.TableData{}, fmt.Errorf("invalid mongo sort JSON: %w", err)
		}
		findOptions.SetSort(sortDoc)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cursor, err := sess.mongoClient.Database(sess.mongoDB).Collection(table).Find(ctx, criteria, findOptions)
	if err != nil {
		return model.TableData{}, err
	}
	defer cursor.Close(ctx)

	rows := []bson.M{}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return model.TableData{}, err
		}
		rows = append(rows, doc)
	}
	if err := cursor.Err(); err != nil {
		return model.TableData{}, err
	}

	return bsonRowsToTable(rows), nil
}

func countMongoRows(sess *session, table, filter string) (int64, error) {
	if table == "" {
		return 0, errors.New("collection is required")
	}
	criteria := bson.M{}
	if strings.TrimSpace(filter) != "" {
		if err := bson.UnmarshalExtJSON([]byte(filter), true, &criteria); err != nil {
			return 0, fmt.Errorf("invalid mongo filter JSON: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return sess.mongoClient.Database(sess.mongoDB).Collection(table).CountDocuments(ctx, criteria)
}

func insertMongoRow(sess *session, table string, row map[string]string) error {
	if table == "" {
		return errors.New("collection is required")
	}
	payload, err := mapToMongoDocument(row)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = sess.mongoClient.Database(sess.mongoDB).Collection(table).InsertOne(ctx, payload)
	return err
}

func updateMongoRow(sess *session, table, keyColumn, keyValue string, row map[string]string) error {
	if table == "" {
		return errors.New("collection is required")
	}
	if keyColumn == "" {
		keyColumn = "_id"
	}
	payload, err := mapToMongoDocument(row)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err = sess.mongoClient.Database(sess.mongoDB).Collection(table).UpdateOne(ctx, bson.M{keyColumn: keyValue}, bson.M{"$set": payload})
	return err
}

func deleteMongoRow(sess *session, table, keyColumn, keyValue string) error {
	if table == "" {
		return errors.New("collection is required")
	}
	if keyColumn == "" {
		keyColumn = "_id"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := sess.mongoClient.Database(sess.mongoDB).Collection(table).DeleteOne(ctx, bson.M{keyColumn: keyValue})
	return err
}

func listMongoObjects(sess *session) ([]model.DBObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	names, err := sess.mongoClient.Database(sess.mongoDB).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	objects := []model.DBObject{{Schema: sess.mongoDB, Name: sess.mongoDB, Type: "database"}}
	for _, name := range names {
		objects = append(objects, model.DBObject{Schema: sess.mongoDB, Name: name, Type: "collection"})
	}
	return objects, nil
}

func listMongoTableInfo(sess *session, _ string) ([]model.TableInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	names, err := sess.mongoClient.Database(sess.mongoDB).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	items := make([]model.TableInfo, 0, len(names))
	for _, name := range names {
		count, err := sess.mongoClient.Database(sess.mongoDB).Collection(name).CountDocuments(ctx, bson.M{})
		if err != nil {
			count = 0
		}
		items = append(items, model.TableInfo{
			Schema:      sess.mongoDB,
			Name:        name,
			Rows:        count,
			Size:        "n/a",
			LastUpdated: "n/a",
		})
	}
	return items, nil
}

func describeMongoColumns(sess *session, table string) ([]model.TableColumn, error) {
	if strings.TrimSpace(table) == "" {
		return nil, errors.New("collection is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var doc bson.M
	err := sess.mongoClient.Database(sess.mongoDB).Collection(table).FindOne(ctx, bson.M{}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return []model.TableColumn{
			{Name: "_id", DataType: "objectid", Nullable: false, IsPrimary: true},
		}, nil
	}
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(doc))
	for key := range doc {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	columns := make([]model.TableColumn, 0, len(keys))
	for _, key := range keys {
		item := model.TableColumn{
			Name:      key,
			DataType:  inferMongoDataType(doc[key]),
			Nullable:  true,
			IsPrimary: key == "_id",
		}
		if key == "_id" {
			item.Nullable = false
		}
		columns = append(columns, item)
	}
	return columns, nil
}

func inferMongoDataType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return "number"
	case time.Time:
		return "timestamp"
	case bson.M, map[string]any:
		return "object"
	case bson.A, []any:
		return "array"
	default:
		typeName := strings.ToLower(fmt.Sprintf("%T", v))
		if strings.Contains(typeName, "objectid") {
			return "objectid"
		}
		if strings.Contains(typeName, "date") || strings.Contains(typeName, "time") {
			return "timestamp"
		}
		return "string"
	}
}

func runRedisCommand(sess *session, query string) (model.QueryResult, error) {
	parts := strings.Fields(query)
	if len(parts) == 0 {
		return model.QueryResult{}, errors.New("empty redis command")
	}

	args := make([]any, len(parts))
	for i, p := range parts {
		args[i] = p
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	value, err := sess.redisClient.Do(ctx, args...).Result()
	if err != nil {
		return model.QueryResult{}, err
	}

	return model.QueryResult{
		Columns: []string{"result"},
		Rows:    [][]string{{fmt.Sprintf("%v", value)}},
		Message: "Redis command executed",
	}, nil
}

func listRedisObjects(sess *session) ([]model.DBObject, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	keys, err := sess.redisClient.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}
	objects := make([]model.DBObject, 0, len(keys)+1)
	objects = append(objects, model.DBObject{Schema: "db", Name: "db", Type: "database"})
	for _, key := range keys {
		typeName, _ := sess.redisClient.Type(ctx, key).Result()
		objects = append(objects, model.DBObject{Schema: "db", Name: key, Type: typeName})
	}
	return objects, nil
}

func listRedisTableInfo(sess *session) ([]model.TableInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	keys, err := sess.redisClient.Keys(ctx, "*").Result()
	if err != nil {
		return nil, err
	}
	items := make([]model.TableInfo, 0, len(keys))
	for _, key := range keys {
		keyType, _ := sess.redisClient.Type(ctx, key).Result()
		size := "n/a"
		if keyType == "string" {
			value, _ := sess.redisClient.Get(ctx, key).Result()
			size = strconv.Itoa(len(value)) + " B"
		}
		items = append(items, model.TableInfo{Schema: "db", Name: key, Rows: 1, Size: size, LastUpdated: "n/a"})
	}
	return items, nil
}

func fetchRedisData(sess *session, pattern string, limit int) (model.TableData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if pattern == "" {
		pattern = "*"
	}
	keys, err := sess.redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		return model.TableData{}, err
	}
	if limit <= 0 {
		limit = 50
	}
	if len(keys) > limit {
		keys = keys[:limit]
	}
	rows := make([][]string, 0, len(keys))
	for _, key := range keys {
		typeName, _ := sess.redisClient.Type(ctx, key).Result()
		value := ""
		if typeName == "string" {
			value, _ = sess.redisClient.Get(ctx, key).Result()
		} else {
			value = "(" + typeName + ")"
		}
		rows = append(rows, []string{key, typeName, value})
	}
	return model.TableData{Columns: []string{"key", "type", "value"}, Rows: rows}, nil
}

func countRedisRows(sess *session, pattern string) (int64, error) {
	if strings.TrimSpace(pattern) == "" {
		pattern = "*"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys, err := sess.redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

func insertRedisValue(sess *session, row map[string]string) error {
	key := row["key"]
	value := row["value"]
	if key == "" {
		return errors.New("redis insert requires key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sess.redisClient.Set(ctx, key, value, 0).Err()
}

func updateRedisValue(sess *session, key string, row map[string]string) error {
	if key == "" {
		key = row["key"]
	}
	if key == "" {
		return errors.New("redis update requires key")
	}
	value := row["value"]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sess.redisClient.Set(ctx, key, value, 0).Err()
}

func deleteRedisValue(sess *session, key string) error {
	if key == "" {
		return errors.New("redis delete requires key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sess.redisClient.Del(ctx, key).Err()
}

func qualifiedTable(dbType model.DBType, schema, table string) string {
	if dbType == model.DBTypeMySQL {
		if schema == "" {
			return quoteIdent(dbType, table)
		}
		return quoteIdent(dbType, schema) + "." + quoteIdent(dbType, table)
	}
	if schema == "" {
		schema = "public"
	}
	return quoteIdent(dbType, schema) + "." + quoteIdent(dbType, table)
}

func quoteIdent(dbType model.DBType, name string) string {
	if dbType == model.DBTypeMySQL {
		return "`" + strings.ReplaceAll(name, "`", "") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, ``) + `"`
}

func placeholder(dbType model.DBType, index int) string {
	if dbType == model.DBTypePostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}

func quotedColumns(dbType model.DBType, columns []string) string {
	items := make([]string, len(columns))
	for i, c := range columns {
		items[i] = quoteIdent(dbType, c)
	}
	return strings.Join(items, ", ")
}

func scanRows(rows *sql.Rows, columns []string) ([][]string, error) {
	resultRows := [][]string{}
	for rows.Next() {
		values := make([]any, len(columns))
		scans := make([]any, len(columns))
		for i := range values {
			scans[i] = &values[i]
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make([]string, len(columns))
		for i, v := range values {
			row[i] = stringify(v)
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resultRows, nil
}

func stringify(v any) string {
	if v == nil {
		return "NULL"
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case time.Time:
		return t.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func inputToDBValue(v string) any {
	if v == dbNullSentinel {
		return nil
	}
	return v
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func mapToMongoDocument(values map[string]string) (bson.M, error) {
	if jsonDoc, ok := values["document"]; ok && strings.TrimSpace(jsonDoc) != "" {
		var doc bson.M
		if err := bson.UnmarshalExtJSON([]byte(jsonDoc), true, &doc); err != nil {
			return nil, fmt.Errorf("invalid document json: %w", err)
		}
		return doc, nil
	}

	doc := bson.M{}
	for k, v := range values {
		if v == dbNullSentinel {
			doc[k] = nil
			continue
		}
		doc[k] = v
	}
	return doc, nil
}

func bsonRowsToTable(rows []bson.M) model.TableData {
	columnSet := map[string]struct{}{}
	for _, row := range rows {
		for key := range row {
			columnSet[key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(columnSet))
	for key := range columnSet {
		columns = append(columns, key)
	}
	sort.Strings(columns)

	resultRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		item := make([]string, len(columns))
		for i, col := range columns {
			item[i] = stringify(row[col])
		}
		resultRows = append(resultRows, item)
	}

	return model.TableData{Columns: columns, Rows: resultRows}
}
