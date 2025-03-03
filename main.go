/*
Package logger provides structured logging capabilities for applications.

This package supports logging to both a database (PostgreSQL) and a rotating log file.
It is designed to capture and store application logs efficiently, including process-specific details.

### Features:
- **Database Logging**: Logs structured data into a PostgreSQL table (`fotadevicelogs`).
- **File-Based Logging**: Uses a rotating log file with configurable retention.
- **Process Metadata**: Captures process ID and creator information for traceability.
- **Schema Management**: Automatically ensures the logging table exists within a specified schema.
- **Error Handling**: Includes stack trace capture for better debugging.

### Usage:

 1. Initialize the logger:
    ```go
    logger, err := logger.InitLogger("postgres://user:pass@localhost/db", "ProcessName", "CreatedBy", "logs/app.log", "public")
    if err != nil {
    log.Fatalf("Logger initialization failed: %v", err)
    }
    defer logger.Close()

 2. Log messages:
    ```go
    logger.Log("1234567890123456", "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890", "info", "Operation successful", map[string]string{"key": "value"}, nil)
    ```

 3. Close the logger when done:
    ```go
    defer logger.Close()
    ```

### Error Handling:
- If the database connection fails, logs are written to the log file only.
- If the log file cannot be written, logs are discarded.
*/
package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	db          *pgxpool.Pool
	logger      *log.Logger
	processID   string
	createdBy   string
	schema      string
	processName string
}

/*
InitLogger initializes the logger with a database connection, file logging, and a process ID.

Parameters:
- `dbURL`: Database connection string.
- `processName`: The name of the process running the logger.
- `createdBy`: Identifier for the user or service creating logs.
- `logFilePath`: Path to the log file.
- `schema`: (Optional) Database schema name; defaults to `"public"`.

Returns:
- A pointer to a `Logger` instance.
- An error if initialization fails.
*/
func InitLogger(dbURL, processName, createdBy, logFilePath, schema string) (*Logger, error) {
	// Ensure the schema is valid, defaulting to "public"
	var defaultSchema string
	if len(schema) <= 3 {
		defaultSchema = "public"
	} else {
		defaultSchema = schema
	}

	// Default log file path if not provided
	if logFilePath == "" {
		execPath, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to get current directory: %v", err)
		}
		logFilePath = filepath.Join(execPath, "applogs.log")
	} else {
		if !strings.HasSuffix(logFilePath, ".log") {
			logFilePath = filepath.Join(logFilePath, "applogs.log")
		}
	}

	// Initialize database connection
	db, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return nil, fmt.Errorf("database connection failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if pingErr := db.Ping(ctx); pingErr != nil {
		return nil, fmt.Errorf("database ping failed: %v", pingErr)
	}

	// Create schema if it doesn't exist
	createSchemaQuery := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, defaultSchema)
	_, err = db.Exec(ctx, createSchemaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema: %v", err)
	}

	//  Create log table if it does not exist
	createTableQuery := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS "%s".fotadevicelogs (
		processid TEXT NOT NULL,
		processname TEXT NOT NULL,
		deviceid TEXT NOT NULL,
		fileid TEXT NOT NULL,
		loglevel TEXT NOT NULL,
		stage TEXT NOT NULL,
		status TEXT NOT NULL,
		metadata JSONB DEFAULT '{}',
		createdby TEXT NOT NULL, 
		createdat BIGINT DEFAULT CAST(extract(epoch FROM NOW()) * 1000 AS BIGINT) NOT NULL,
		PRIMARY KEY (processid, deviceid, fileid , processname, createdat));`, defaultSchema)

	_, err = db.Exec(ctx, createTableQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to create log table: %v", err)
	}

	// Configure file-based logging
	logFile := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    10,   // MB
		MaxBackups: 5,    // Number of old logs to retain
		MaxAge:     30,   // Days
		Compress:   true, // Enable compression
	}

	loggerInstance := log.New(logFile, "", log.LstdFlags|log.Lshortfile)
	pid := os.Getpid()

	return &Logger{
		db:          db,
		logger:      loggerInstance,
		processID:   fmt.Sprintf("%d", pid),
		processName: processName,
		createdBy:   createdBy,
		schema:      defaultSchema,
	}, nil
}

/*
LogToDB inserts a log entry into the database.
*/
func (l *Logger) LogToDB(deviceID, fileID, stage, status, logLevel string, metadata interface{}) {
	if l.db == nil {
		l.logger.Println("DB logging skipped: No database connection")
		return
	}

	logLevel = strings.ToUpper(logLevel)

	// Validate input parameters
	if len(deviceID) != 16 || len(fileID) != 64 {
		l.logger.Println("Invalid log: Device ID must be 16 digits, File ID must be 64 digits")
		return
	}

	validLogLevels := map[string]bool{"INFO": true, "WARN": true, "ERROR": true, "DEBUG": true}
	if !validLogLevels[logLevel] {
		l.logger.Println("Invalid log: Log level must be one of: INFO, WARN, ERROR, DEBUG")
		return
	}

	var compressedMetadata []byte
	if metadata == nil {
		compressedMetadata = []byte("{}") // Assign an empty JSON object
	} else {
		data, err := json.Marshal(metadata)
		if err != nil {
			l.logger.Printf("Failed to compress summary: %v\n", err)
			return
		}
		compressedMetadata = data
	}

	query := fmt.Sprintf(`
		INSERT INTO "%s".fotadevicelogs (processid, processname, deviceid, fileid, stage, status, loglevel, createdby, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, l.schema)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, dbErr := l.db.Exec(ctx, query, l.processID, l.processName, deviceID, fileID, stage, status, logLevel, l.createdBy, compressedMetadata)
	if dbErr != nil {
		l.logger.Printf("Failed to insert log into DB: %v\n", dbErr)
	}
}

/*
Log writes a log message to both the log file and the database.

This function logs a structured message that includes the device ID, file ID, log level, and status.
If an error is provided, it captures and appends a stack trace to the log message.
It then writes the log to both the file and the database.

### Parameters:
- `deviceID` (string): Unique identifier of the device. Must be exactly 16 characters.
- `fileID` (string): Unique identifier of the file. Must be exactly 64 characters.
- `logLevel` (string): Log severity level. Must be one of "info", "warn", "error", or "debug".
- `status` (string): Status message associated with the log entry.
- `metadata` (interface{}): Additional structured data related to the log entry (can be a map, struct, etc.).
- `err` (error): Optional error object. If provided, the function captures the error stack trace.

### Behavior:
1. Formats a log message including the provided parameters.
2. Captures the stack trace if an error is provided.
3. Writes the log message to the configured log file.
4. Calls `LogToDB` to store the log entry in the database.

### Example Usage:
```go
logger.Log("1234567890123456", "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890", "error", "Failed to process file", map[string]string{"key": "value"}, errors.New("file not found"))
```
*/
func (l *Logger) Log(deviceID, fileID, stage, status, logLevel string, metadata interface{}) {
	logMessage := fmt.Sprintf("[%s] DeviceId: %s, FileId: %s, Stage: %s, Status: %s", logLevel, deviceID, fileID, stage, status)

	l.logger.Println(logMessage)

	l.LogToDB(deviceID, fileID, stage, status, logLevel, metadata)
}

/*
Close closes the database connection.
*/
func (l *Logger) Close() {
	if l.db != nil {
		l.db.Close()
	}
}

// akash
// get log level for the status
var LogLevelMap = make(map[string]string)

func getLogLevel(status string) string {
	if logLevel, ok := LogLevelMap[status]; ok {
		return logLevel
	}
	return "INFO"
}

var LoggedStages = make(map[string]map[string]bool)

func logOnce(loggerI *Logger, deviceid string, fileId, stageName, status string, metadata interface{}) {
	if _, exists := LoggedStages[deviceid]; !exists {
		LoggedStages[deviceid] = make(map[string]bool)
	}
	logKey := fmt.Sprintf("%s:%s", stageName, status)

	logLevel := getLogLevel(status)

	if !LoggedStages[deviceid][logKey] {
		loggerI.Log(deviceid, fileId, stageName, status, logLevel, metadata)
		LoggedStages[deviceid][logKey] = true
	}
}

// for checking device stage
var DeviceStageMap = make(map[string]string)

func (l *Logger) UpdateStageAndLog(deviceid, newStage, fileId, status string, metadata interface{}) {
	DeviceStageMap[deviceid] = newStage
	logOnce(l, deviceid, fileId, newStage, status, metadata)
}

// for comparing the failure time and logging the stage
func (l *Logger) CreateMapToCheckFailure() (map[string]int64, map[string]int64) {
	deviceTimeMap := make(map[string]int64)
	deviceTimeMap2 := make(map[string]int64)
	return deviceTimeMap, deviceTimeMap2
}
