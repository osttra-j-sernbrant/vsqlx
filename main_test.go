package main

import (
	"os"
	"testing"
	"time"
)

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		port     int
		user     string
		password string
		db       string
		tlsMode  string
		expected string
	}{
		{
			name:     "basic with password",
			host:     "localhost",
			port:     5433,
			user:     "dbadmin",
			password: "password123",
			db:       "mydb",
			tlsMode:  "prefer",
			expected: "vertica://dbadmin:password123@localhost:5433/mydb?tlsmode=prefer",
		},
		{
			name:     "no password",
			host:     "localhost",
			port:     5433,
			user:     "dbadmin",
			password: "",
			db:       "mydb",
			tlsMode:  "server",
			expected: "vertica://dbadmin@localhost:5433/mydb?tlsmode=server",
		},
		{
			name:     "special characters",
			host:     "remote-host",
			port:     1234,
			user:     "user/name",
			password: "pass@word",
			db:       "db-name",
			tlsMode:  "none",
			expected: "vertica://user%2Fname:pass%40word@remote-host:1234/db-name?tlsmode=none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDSN(tt.host, tt.port, tt.user, tt.password, tt.db, tt.tlsMode)
			if got != tt.expected {
				t.Errorf("buildDSN() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestToString(t *testing.T) {
	now := time.Now()
	tests := []struct {
		input    any
		expected string
	}{
		{nil, "NULL"},
		{"hello", "hello"},
		{[]byte("world"), "world"},
		{123, "123"},
		{true, "true"},
		{now, now.Format(time.DateTime)},
	}

	conv := buildStringConverter(nil)
	for _, tt := range tests {
		got := conv(tt.input)
		if got != tt.expected {
			t.Errorf("buildStringConverter(nil)(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestToJSONValue(t *testing.T) {
	now := time.Now()
	tests := []struct {
		input    any
		expected any
	}{
		{nil, nil},
		{"hello", "hello"},
		{[]byte("world"), "world"},
		{123, 123},
		{now, now.Format(time.RFC3339)},
	}

	for _, tt := range tests {
		got := toJSONValue(tt.input)
		if got != tt.expected {
			t.Errorf("toJSONValue(%v) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestGetPasswordFromPgpass(t *testing.T) {
	tempFile, err := os.CreateTemp("", "pgpass-test")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := `
# some comments
localhost:5433:mydb:dbadmin:secret_pass
*:5435:*:user_abc:wildcard_pass
remote-host:*:*:*:port_wildcard_pass
`
	if _, err := tempFile.WriteString(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tempFile.Close()

	t.Setenv("PGPASSFILE", tempFile.Name())

	tests := []struct {
		name      string
		host      string
		port      string
		dbname    string
		user      string
		expected  string
		expectErr bool
	}{
		{
			name:      "exact match",
			host:      "localhost",
			port:      "5433",
			dbname:    "mydb",
			user:      "dbadmin",
			expected:  "secret_pass",
			expectErr: false,
		},
		{
			name:      "wildcard host, port, db",
			host:      "otherhost",
			port:      "5435",
			dbname:    "otherdb",
			user:      "user_abc",
			expected:  "wildcard_pass",
			expectErr: false,
		},
		{
			name:      "wildcard port, db, user",
			host:      "remote-host",
			port:      "9999",
			dbname:    "xyz",
			user:      "admin",
			expected:  "port_wildcard_pass",
			expectErr: false,
		},
		{
			name:      "no match",
			host:      "other-host",
			port:      "1111",
			dbname:    "unknown",
			user:      "unknown",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getPasswordFromPgpass(tt.host, tt.port, tt.dbname, tt.user)
			if (err != nil) != tt.expectErr {
				t.Fatalf("getPasswordFromPgpass() error = %v, expectErr %v", err, tt.expectErr)
			}
			if !tt.expectErr && got != tt.expected {
				t.Errorf("getPasswordFromPgpass() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestConvertSQLValueForParquet_NilColumnType(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"nil value", nil, nil},
		{"string value", "hello", "hello"},
		{"int value", 123, 123},
		{"byte slice value", []byte("world"), "world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conv := buildConverter(nil)
			got := conv(tt.input)
			if got != tt.expected {
				t.Errorf("buildConverter(nil)() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFormatFloatVal(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"zero", 0.0, "0"},
		{"normal positive", 123.456, "123.456"},
		{"normal negative", -1138558.48868431, "-1138558.48868431"},
		{"extreme large", -1.98e+126, "-1.98e+126"},
		{"extreme small", 1.23e-06, "1.23e-06"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatFloatVal(tt.input)
			if got != tt.expected {
				t.Errorf("formatFloatVal(%v) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestCenterString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		width    int
		expected string
	}{
		{"even padding", "test", 8, "  test  "},
		{"odd padding extra right", "test", 7, " test  "},
		{"no padding needed", "super-long-string", 5, "super-long-string"},
		{"exact width", "test", 4, "test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := centerString(tt.input, tt.width)
			if got != tt.expected {
				t.Errorf("centerString(%q, %d) = %q, want %q", tt.input, tt.width, got, tt.expected)
			}
		})
	}
}
