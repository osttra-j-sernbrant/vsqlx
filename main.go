package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	_ "github.com/vertica/vertica-sql-go"
	vlogger "github.com/vertica/vertica-sql-go/logger"
)

func init() {
	// Silence non-critical driver warning messages (like late-arriving packets after cancellation)
	vlogger.SetLogLevel(vlogger.ERROR)
}

func loadEnv() {
	file, err := os.Open(".env")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
			val = val[1 : len(val)-1]
		} else if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
			val = val[1 : len(val)-1]
		}

		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

func buildDSN(host string, port int, user, password, db, tlsMode string) string {
	queryParams := url.Values{}
	queryParams.Add("tlsmode", tlsMode)

	var userInfo *url.Userinfo
	if password != "" {
		userInfo = url.UserPassword(user, password)
	} else {
		userInfo = url.User(user)
	}

	u := url.URL{
		Scheme:   "vertica",
		User:     userInfo,
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     db,
		RawQuery: queryParams.Encode(),
	}
	return u.String()
}

func buildStringConverter(ct *sql.ColumnType) func(any) string {
	if ct == nil {
		return func(val any) string {
			if val == nil {
				return "NULL"
			}
			switch v := val.(type) {
			case float64:
				return strconv.FormatFloat(v, 'f', -1, 64)
			case float32:
				return strconv.FormatFloat(float64(v), 'f', -1, 32)
			case []byte:
				return string(v)
			case time.Time:
				return v.Format(time.DateTime)
			}
			return fmt.Sprintf("%v", val)
		}
	}

	dbTypeName := strings.ToUpper(ct.DatabaseTypeName())

	if dbTypeName == "DATE" {
		return func(val any) string {
			if val == nil {
				return "NULL"
			}
			switch v := val.(type) {
			case time.Time:
				return v.Format(time.DateOnly)
			case string:
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t.Format(time.DateOnly)
				}
				return v
			case []byte:
				s := string(v)
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					return t.Format(time.DateOnly)
				}
				return s
			}
			return fmt.Sprintf("%v", val)
		}
	}

	if strings.Contains(dbTypeName, "TIME") || strings.Contains(dbTypeName, "TIMESTAMP") {
		return func(val any) string {
			if val == nil {
				return "NULL"
			}
			switch v := val.(type) {
			case time.Time:
				return v.Format(time.DateTime)
			case string:
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t.Format(time.DateTime)
				}
				return v
			case []byte:
				s := string(v)
				if t, err := time.Parse(time.RFC3339, s); err == nil {
					return t.Format(time.DateTime)
				}
				return s
			}
			return fmt.Sprintf("%v", val)
		}
	}

	return func(val any) string {
		if val == nil {
			return "NULL"
		}
		switch v := val.(type) {
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64)
		case float32:
			return strconv.FormatFloat(float64(v), 'f', -1, 32)
		case []byte:
			return string(v)
		case time.Time:
			return v.Format(time.DateTime)
		}
		return fmt.Sprintf("%v", val)
	}
}

func toJSONValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339)
	default:
		return v
	}
}

func buildConverter(ct *sql.ColumnType) func(any) any {
	if ct == nil {
		return func(val any) any {
			if val == nil {
				return nil
			}
			if b, ok := val.([]byte); ok {
				return string(b)
			}
			return val
		}
	}

	dbTypeName := strings.ToUpper(ct.DatabaseTypeName())

	if strings.Contains(dbTypeName, "INT") {
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case int64:
				return v
			case int:
				return int64(v)
			case sql.NullInt64:
				if v.Valid {
					return v.Int64
				}
				return nil
			case []byte:
				i, _ := strconv.ParseInt(string(v), 10, 64)
				return i
			case string:
				i, _ := strconv.ParseInt(v, 10, 64)
				return i
			}
			rVal := reflect.ValueOf(val)
			if rVal.Kind() == reflect.Ptr {
				if rVal.IsNil() {
					return nil
				}
				return reflect.Indirect(rVal).Convert(reflect.TypeOf(int64(0))).Interface()
			}
			return rVal.Convert(reflect.TypeOf(int64(0))).Interface()
		}
	}

	if strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") || strings.Contains(dbTypeName, "REAL") || strings.Contains(dbTypeName, "NUMERIC") || strings.Contains(dbTypeName, "DECIMAL") {
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case float64:
				return v
			case float32:
				return float64(v)
			case sql.NullFloat64:
				if v.Valid {
					return v.Float64
				}
				return nil
			case []byte:
				f, _ := strconv.ParseFloat(string(v), 64)
				return f
			case string:
				f, _ := strconv.ParseFloat(v, 64)
				return f
			}
			rVal := reflect.ValueOf(val)
			if rVal.Kind() == reflect.Ptr {
				if rVal.IsNil() {
					return nil
				}
				return reflect.Indirect(rVal).Convert(reflect.TypeOf(float64(0.0))).Interface()
			}
			return rVal.Convert(reflect.TypeOf(float64(0.0))).Interface()
		}
	}

	if strings.Contains(dbTypeName, "BOOL") {
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case bool:
				return v
			case sql.NullBool:
				if v.Valid {
					return v.Bool
				}
				return nil
			case int64:
				return v != 0
			case int:
				return v != 0
			case string:
				b, _ := strconv.ParseBool(v)
				return b
			case []byte:
				b, _ := strconv.ParseBool(string(v))
				return b
			}
			return false
		}
	}

	if strings.Contains(dbTypeName, "TIME") || strings.Contains(dbTypeName, "DATE") {
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case time.Time:
				return v
			case sql.NullTime:
				if v.Valid {
					return v.Time
				}
				return nil
			case string:
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t
				}
			case []byte:
				if t, err := time.Parse(time.RFC3339, string(v)); err == nil {
					return t
				}
			}
			return nil
		}
	}

	if ct.ScanType() == nil {
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case string:
				return v
			case []byte:
				return string(v)
			case sql.NullString:
				if v.Valid {
					return v.String
				}
				return nil
			}
			return fmt.Sprintf("%v", val)
		}
	}

	kind := ct.ScanType().Kind()
	scanTypeStr := ct.ScanType().String()

	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case int64:
				return v
			case int:
				return int64(v)
			case sql.NullInt64:
				if v.Valid {
					return v.Int64
				}
				return nil
			case []byte:
				i, _ := strconv.ParseInt(string(v), 10, 64)
				return i
			case string:
				i, _ := strconv.ParseInt(v, 10, 64)
				return i
			}
			rVal := reflect.ValueOf(val)
			if rVal.Kind() == reflect.Ptr {
				if rVal.IsNil() {
					return nil
				}
				return reflect.Indirect(rVal).Convert(reflect.TypeOf(int64(0))).Interface()
			}
			return rVal.Convert(reflect.TypeOf(int64(0))).Interface()
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case uint64:
				return int64(v)
			case uint:
				return int64(v)
			}
			rVal := reflect.ValueOf(val)
			if rVal.Kind() == reflect.Ptr {
				if rVal.IsNil() {
					return nil
				}
				return reflect.Indirect(rVal).Convert(reflect.TypeOf(int64(0))).Interface()
			}
			return rVal.Convert(reflect.TypeOf(int64(0))).Interface()
		}

	case reflect.Float32, reflect.Float64:
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case float64:
				return v
			case float32:
				return float64(v)
			case sql.NullFloat64:
				if v.Valid {
					return v.Float64
				}
				return nil
			case []byte:
				f, _ := strconv.ParseFloat(string(v), 64)
				return f
			case string:
				f, _ := strconv.ParseFloat(v, 64)
				return f
			}
			rVal := reflect.ValueOf(val)
			if rVal.Kind() == reflect.Ptr {
				if rVal.IsNil() {
					return nil
				}
				return reflect.Indirect(rVal).Convert(reflect.TypeOf(float64(0.0))).Interface()
			}
			return rVal.Convert(reflect.TypeOf(float64(0.0))).Interface()
		}

	case reflect.Bool:
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case bool:
				return v
			case sql.NullBool:
				if v.Valid {
					return v.Bool
				}
				return nil
			case int64:
				return v != 0
			case int:
				return v != 0
			case string:
				b, _ := strconv.ParseBool(v)
				return b
			case []byte:
				b, _ := strconv.ParseBool(string(v))
				return b
			}
			return false
		}

	case reflect.Struct:
		if scanTypeStr == "time.Time" {
			return func(val any) any {
				if val == nil {
					return nil
				}
				switch v := val.(type) {
				case time.Time:
					return v
				case sql.NullTime:
					if v.Valid {
						return v.Time
					}
					return nil
				case string:
					if t, err := time.Parse(time.RFC3339, v); err == nil {
						return t
					}
				case []byte:
					if t, err := time.Parse(time.RFC3339, string(v)); err == nil {
						return t
					}
				}
				return nil
			}
		}
		fallthrough

	default:
		return func(val any) any {
			if val == nil {
				return nil
			}
			switch v := val.(type) {
			case string:
				return v
			case []byte:
				return string(v)
			case sql.NullString:
				if v.Valid {
					return v.String
				}
				return nil
			}
			return fmt.Sprintf("%v", val)
		}
	}
}

func readSQLFile(path string) (string, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func readStdin() (string, error) {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		return "", nil
	}
	bytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func getPasswordFromPgpass(host, port, dbname, user string) (string, error) {
	pgpassPath := os.Getenv("PGPASSFILE")
	if pgpassPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		pgpassPath = filepath.Join(home, ".pgpass")
	}

	file, err := os.Open(pgpassPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 5)
		if len(parts) != 5 {
			continue
		}

		match := func(val, pattern string) bool {
			return pattern == "*" || val == pattern
		}

		if match(host, parts[0]) && match(port, parts[1]) && match(dbname, parts[2]) && match(user, parts[3]) {
			password := parts[4]
			password = strings.ReplaceAll(password, `\:`, ":")
			password = strings.ReplaceAll(password, `\\`, `\`)
			return password, nil
		}
	}

	return "", fmt.Errorf("no matching entry found in %s", pgpassPath)
}

func formatTable(w io.Writer, cols []string, rows *sql.Rows, colTypes []*sql.ColumnType) error {
	converters := make([]func(any) string, len(colTypes))
	for i, ct := range colTypes {
		converters[i] = buildStringConverter(ct)
	}

	var bufferedRows [][]string
	colWidths := make([]int, len(cols))
	for i, col := range cols {
		colWidths[i] = len(col)
	}

	scanArgs := make([]any, len(cols))
	values := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		rowVals := make([]string, len(cols))
		for i := range values {
			strVal := converters[i](values[i])
			rowVals[i] = strVal
			if len(strVal) > colWidths[i] {
				colWidths[i] = len(strVal)
			}
		}
		bufferedRows = append(bufferedRows, rowVals)
		rowCount++
	}

	var headerParts []string
	for i, col := range cols {
		headerParts = append(headerParts, fmt.Sprintf(" %-*s ", colWidths[i], col))
	}
	fmt.Fprintln(w, strings.Join(headerParts, "|"))

	var dividerParts []string
	for _, width := range colWidths {
		dividerParts = append(dividerParts, strings.Repeat("-", width+2))
	}
	fmt.Fprintln(w, strings.Join(dividerParts, "+"))

	for _, rowVals := range bufferedRows {
		var rowParts []string
		for i, val := range rowVals {
			isNumeric := false
			if i < len(colTypes) {
				dbTypeName := strings.ToUpper(colTypes[i].DatabaseTypeName())
				if strings.Contains(dbTypeName, "INT") || strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") || strings.Contains(dbTypeName, "REAL") || strings.Contains(dbTypeName, "NUMERIC") || strings.Contains(dbTypeName, "DECIMAL") {
					isNumeric = true
				}
			}

			if isNumeric {
				rowParts = append(rowParts, fmt.Sprintf(" %*s ", colWidths[i], val))
			} else {
				rowParts = append(rowParts, fmt.Sprintf(" %-*s ", colWidths[i], val))
			}
		}
		fmt.Fprintln(w, strings.Join(rowParts, "|"))
	}

	fmt.Fprintf(w, "(%d rows)\n", rowCount)
	return nil
}

func formatCSV(w io.Writer, cols []string, rows *sql.Rows, colTypes []*sql.ColumnType) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}

	converters := make([]func(any) string, len(colTypes))
	for i, ct := range colTypes {
		converters[i] = buildStringConverter(ct)
	}

	scanArgs := make([]any, len(cols))
	values := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		rowVals := make([]string, len(cols))
		for i := range values {
			rowVals[i] = converters[i](values[i])
		}
		if err := cw.Write(rowVals); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func formatJSON(w io.Writer, cols []string, rows *sql.Rows) error {
	scanArgs := make([]interface{}, len(cols))
	values := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		rowMap := make(map[string]interface{})
		for i, col := range cols {
			rowMap[col] = toJSONValue(values[i])
		}
		results = append(results, rowMap)
	}

	if results == nil {
		results = []map[string]interface{}{}
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

type orderedGroup struct {
	parquet.Node
	orderedFields []parquet.Field
}

func (o orderedGroup) Fields() []parquet.Field {
	return o.orderedFields
}

func formatParquet(w io.Writer, cols []string, rows *sql.Rows, colTypes []*sql.ColumnType) error {
	group := parquet.Group{}
	for _, ct := range colTypes {
		var node parquet.Node
		dbTypeName := strings.ToUpper(ct.DatabaseTypeName())

		if strings.Contains(dbTypeName, "INT") {
			node = parquet.Int(64)
		} else if strings.Contains(dbTypeName, "FLOAT") || strings.Contains(dbTypeName, "DOUBLE") || strings.Contains(dbTypeName, "REAL") || strings.Contains(dbTypeName, "NUMERIC") || strings.Contains(dbTypeName, "DECIMAL") {
			node = parquet.Leaf(parquet.DoubleType)
		} else if strings.Contains(dbTypeName, "BOOL") {
			node = parquet.Leaf(parquet.BooleanType)
		} else if strings.Contains(dbTypeName, "TIME") || strings.Contains(dbTypeName, "DATE") {
			node = parquet.Timestamp(parquet.Microsecond)
		} else if ct.ScanType() != nil {
			switch ct.ScanType().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				node = parquet.Int(64)
			case reflect.Float32, reflect.Float64:
				node = parquet.Leaf(parquet.DoubleType)
			case reflect.Bool:
				node = parquet.Leaf(parquet.BooleanType)
			case reflect.Struct:
				if ct.ScanType().String() == "time.Time" {
					node = parquet.Timestamp(parquet.Microsecond)
				} else {
					node = parquet.String()
				}
			default:
				node = parquet.String()
			}
		} else {
			node = parquet.String()
		}
		group[ct.Name()] = parquet.Optional(node)
	}

	fields := group.Fields()
	fieldMap := make(map[string]parquet.Field)
	for _, f := range fields {
		fieldMap[f.Name()] = f
	}

	orderedFields := make([]parquet.Field, 0, len(colTypes))
	for _, ct := range colTypes {
		if f, ok := fieldMap[ct.Name()]; ok {
			orderedFields = append(orderedFields, f)
		}
	}

	root := orderedGroup{
		Node:          group,
		orderedFields: orderedFields,
	}

	schema := parquet.NewSchema("query_results", root)

	writer := parquet.NewGenericWriter[any](w, schema)
	defer writer.Close()

	// Pre-compile column converters to avoid reflection inside the loop
	converters := make([]func(any) any, len(colTypes))
	for i, ct := range colTypes {
		converters[i] = buildConverter(ct)
	}

	scanArgs := make([]any, len(cols))
	values := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var batch []any
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		rowMap := make(map[string]any)
		for i, ct := range colTypes {
			rowMap[ct.Name()] = converters[i](values[i])
		}
		batch = append(batch, rowMap)

		if len(batch) >= 10000 {
			if _, err := writer.Write(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if _, err := writer.Write(batch); err != nil {
			return err
		}
	}

	return writer.Close()
}

func main() {
	loadEnv()

	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = os.Getenv("USERNAME")
	}
	if currentUser == "" {
		currentUser = "dbadmin"
	}

	defaultHost := os.Getenv("VERTICA_HOST")
	if defaultHost == "" {
		defaultHost = "localhost"
	}
	defaultPort := 5433
	if portStr := os.Getenv("VERTICA_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			defaultPort = p
		}
	}
	defaultUser := os.Getenv("VERTICA_USER")
	if defaultUser == "" {
		defaultUser = currentUser
	}
	defaultPassword := os.Getenv("VERTICA_PASSWORD")
	defaultDB := os.Getenv("VERTICA_DB")
	if defaultDB == "" {
		defaultDB = currentUser
	}
	defaultTLSMode := os.Getenv("VERTICA_TLSMODE")
	if defaultTLSMode == "" {
		defaultTLSMode = "prefer"
	}

	var (
		host       string
		port       int
		user       string
		password   string
		dbName     string
		tlsMode    string
		query      string
		file       string
		format     string
		outputPath string
		timeout    time.Duration
		verbose    bool
	)

	// Connection and execution options (matching vsql short flags exactly)
	flag.StringVar(&host, "h", defaultHost, "Database server host")
	flag.IntVar(&port, "p", defaultPort, "Database server port")
	flag.StringVar(&user, "U", defaultUser, "Database user name")
	flag.StringVar(&password, "w", defaultPassword, "Database user password")
	flag.StringVar(&dbName, "d", defaultDB, "Database name")
	flag.StringVar(&tlsMode, "m", defaultTLSMode, "SSL mode (verify-full, require, prefer, allow, disable)")
	flag.StringVar(&query, "c", "", "SQL query to execute")
	flag.StringVar(&file, "f", "", "Path to a file containing the SQL query")
	flag.StringVar(&outputPath, "o", "", "Output file path (optional, defaults to stdout)")

	// Format extensions (non-vsql flags)
	flag.StringVar(&format, "format", "table", "Output format: table, json, csv, parquet")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "Query timeout duration")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose driver warning and error logs")
	flag.BoolVar(&verbose, "v", false, "Enable verbose driver warning and error logs (shorthand)")

	flag.Parse()

	if verbose {
		vlogger.SetLogLevel(vlogger.WARN)
	}

	var queryStr string
	if query != "" {
		queryStr = query
	} else if file != "" {
		var err error
		queryStr, err = readSQLFile(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to read SQL file: %v\n", err)
			os.Exit(1)
		}
	} else {
		var err error
		queryStr, err = readStdin()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to read from stdin: %v\n", err)
			os.Exit(1)
		}
	}

	queryStr = strings.TrimSpace(queryStr)
	if queryStr == "" {
		fmt.Fprintln(os.Stderr, "Error: No query provided. Use -query (-c), -file (-f), or pipe a query to stdin.")
		os.Exit(1)
	}

	pass := password
	if pass == "" {
		if p, err := getPasswordFromPgpass(host, strconv.Itoa(port), dbName, user); err == nil {
			pass = p
		}
	}

	dsn := buildDSN(host, port, user, pass, dbName, tlsMode)

	db, err := sql.Open("vertica", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to open connection: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		if err == context.Canceled || strings.Contains(err.Error(), "canceled") {
			fmt.Fprintln(os.Stderr, "Error: Connection attempt was canceled.")
		} else if err == context.DeadlineExceeded || strings.Contains(err.Error(), "deadline exceeded") {
			fmt.Fprintf(os.Stderr, "Error: Connection to Vertica timed out after %s.\n", timeout)
		} else if err == io.EOF || strings.Contains(err.Error(), "EOF") {
			fmt.Fprintln(os.Stderr, "Error: Failed to connect to Vertica: EOF. (Please verify your username, password, or host proxy settings as Vertica or the connection proxy will abruptly close the connection on authentication failure.)")
		} else if strings.Contains(err.Error(), "28000") || strings.Contains(err.Error(), "Invalid username or password") || strings.Contains(err.Error(), "authentication failed") {
			fmt.Fprintln(os.Stderr, "Error: Failed to connect to Vertica: Invalid username or password (SQLState 28000).")
		} else {
			fmt.Fprintf(os.Stderr, "Error: Failed to connect to Vertica: %v\n", err)
		}
		os.Exit(1)
	}

	rows, err := db.QueryContext(ctx, queryStr)
	if err != nil {
		if err == context.Canceled || strings.Contains(err.Error(), "canceled") {
			fmt.Fprintln(os.Stderr, "Error: Query was canceled.")
		} else if err == context.DeadlineExceeded || strings.Contains(err.Error(), "deadline exceeded") {
			fmt.Fprintf(os.Stderr, "Error: Query execution timed out after %s.\n", timeout)
		} else {
			fmt.Fprintf(os.Stderr, "Error: Query failed: %v\n", err)
		}
		os.Exit(1)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get columns: %v\n", err)
		os.Exit(1)
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get column types: %v\n", err)
		os.Exit(1)
	}

	var output io.Writer = os.Stdout
	if outputPath != "" {
		file, err := os.Create(outputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to create output file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		output = file
	} else if strings.ToLower(format) == "parquet" {
		stat, _ := os.Stdout.Stat()
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			fmt.Fprintln(os.Stderr, "Error: Writing binary Parquet data to a terminal is not allowed. Please specify an output file with -output (-o) or redirect stdout.")
			os.Exit(1)
		}
	}

	switch strings.ToLower(format) {
	case "json":
		err = formatJSON(output, cols, rows)
	case "csv":
		err = formatCSV(output, cols, rows, colTypes)
	case "table":
		err = formatTable(output, cols, rows, colTypes)
	case "parquet":
		err = formatParquet(output, cols, rows, colTypes)
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown format %q. Supported formats: table, json, csv, parquet\n", format)
		os.Exit(1)
	}

	if err != nil {
		if err == context.Canceled || strings.Contains(err.Error(), "canceled") {
			fmt.Fprintln(os.Stderr, "Error: Query was canceled.")
		} else if err == context.DeadlineExceeded || strings.Contains(err.Error(), "deadline exceeded") {
			fmt.Fprintf(os.Stderr, "Error: Query timed out after %s.\n", timeout)
		} else {
			fmt.Fprintf(os.Stderr, "Error: Failed to format output: %v\n", err)
		}
		os.Exit(1)
	}
}
