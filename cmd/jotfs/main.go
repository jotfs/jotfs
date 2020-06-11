package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jotfs/jotfs/internal/db"
	pb "github.com/jotfs/jotfs/internal/protos"
	"github.com/jotfs/jotfs/internal/server"
	"github.com/jotfs/jotfs/internal/store"
	"github.com/jotfs/jotfs/internal/store/s3"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"github.com/twitchtv/twirp"
)

// Build flags
var (
	Version   string
	BuildDate string
	CommitID  string
)

const (
	defaultDatabase         = "./jotfs.db"
	defaultPort             = 6777
	defaultLogLevel         = "warn"
	defaultDLTimeoutMinutes = 120

	defaultStoreEndpoint = "s3.amazonaws.com"
	defaultRegion        = "us-east-1"

	kiB = 1024
	miB = 1024 * kiB

	maxPackfileSize = 128 * miB

	minAvgKib            = 64
	maxAvgKib            = 64 * 1024 // 64 MiB
	defaultAvgKib        = 512
	defaultNormalization = 2

	chunkParamsKey = "params.json"
)

type serverConfig struct {
	Port              uint
	Database          string
	VersioningEnabled bool
	AvgChunkKiB       uint
	LogLevel          string
	TLSCert           string
	TLSKey            string
	DLTimeoutMinutes  uint
}

type storeConfig struct {
	AccessKey  string
	SecretKey  string
	Bucket     string
	Region     string
	DisableSSL bool
	PathStyle  bool
	Endpoint   string
}

type config struct {
	Server *serverConfig
	Store  *storeConfig
}

func openDB(filename string) (*db.Adapter, error) {
	exists, err := fileExists(filename)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %v", filename, err)
	}
	if exists {
		fmt.Printf("Using existing database %s\n", filename)
	} else {
		fmt.Printf("Creating new database %s\n", filename)
	}
	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_fk=true", filename))
	if err != nil {
		return nil, err
	}
	if err := sqldb.Ping(); err != nil {
		return nil, fmt.Errorf("could not connect")
	}
	adapter := db.NewAdapter(sqldb)
	if !exists {
		if err := adapter.InitSchema(); err != nil {
			return nil, fmt.Errorf("internal error: creating database schema: %v", err)
		}
	}
	return adapter, nil
}

func fileExists(f string) (bool, error) {
	info, err := os.Stat(f)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.IsDir() {
		return false, errors.New("is a directory but a file is required")
	}
	return true, nil
}

func requiredFlagError(flag string) error {
	return fmt.Errorf("flag -%s is reqiured", flag)
}

func (c serverConfig) validate() error {
	if c.AvgChunkKiB < minAvgKib || c.AvgChunkKiB > maxAvgKib {
		return fmt.Errorf("-chunk_size must be in range %d to %d", minAvgKib, maxAvgKib)
	}
	if (c.TLSCert == "" && c.TLSKey != "") || (c.TLSCert != "" && c.TLSKey == "") {
		return fmt.Errorf("flags -ssl_cert and -ssl_key must be provided together")
	}
	switch c.LogLevel {
	case "", "debug", "info", "warn", "error":
		break
	default:
		return fmt.Errorf("invalid -log_level %q. Must be one of: debug, info, warn, error", c.LogLevel)
	}
	return nil
}

func (c storeConfig) validate() error {
	if c.Bucket == "" {
		return requiredFlagError("store_bucket")
	}
	return nil
}

func loggingServerHooks() *twirp.ServerHooks {
	hooks := &twirp.ServerHooks{}

	// Define a key type to keep context.WithValue happy
	type key int
	const (
		receivedAtKey key = iota + 1
		msgKey
		reqIDKey
	)

	hooks.RequestReceived = func(ctx context.Context) (context.Context, error) {
		reqID := xid.New().String()
		twirp.SetHTTPResponseHeader(ctx, "x-jotfs-request-id", reqID)
		ctx = context.WithValue(ctx, reqIDKey, xid.New().String())
		ctx = context.WithValue(ctx, receivedAtKey, time.Now())
		return ctx, nil
	}

	hooks.Error = func(ctx context.Context, err twirp.Error) context.Context {
		ctx = context.WithValue(ctx, msgKey, err.Error())
		return ctx
	}

	hooks.ResponseSent = func(ctx context.Context) {
		var reqID string
		if v := ctx.Value(reqIDKey); v != nil {
			reqID = v.(string)
		}
		var elapsed time.Duration
		if v := ctx.Value(receivedAtKey); v != nil {
			elapsed = time.Since(v.(time.Time))
		}
		var msg string
		if v := ctx.Value(msgKey); v != nil {
			msg = v.(string)
		}
		method, _ := twirp.MethodName(ctx)
		code, _ := twirp.StatusCode(ctx)
		status, _ := strconv.Atoi(code)

		rpcLogger := logger.With().
			Str("method", method).
			Int("status", status).
			Int64("elapsed", elapsed.Milliseconds()).
			Str("id", reqID).
			Logger()

		if 200 <= status && status < 300 {
			rpcLogger.Info().Msg(msg)
		} else if 400 <= status && status < 500 {
			rpcLogger.Warn().Msg(msg)
		} else {
			rpcLogger.Error().Msg(msg)
		}
	}

	return hooks
}

// getChunkerParams gets the chunker parameters from the store. Return nil if the file
// does not exist.
func getChunkerParams(ctx context.Context, s store.Store, bucket string) (*server.ChunkerParams, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	r, err := s.Get(ctx, bucket, chunkParamsKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var params server.ChunkerParams
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading object: %v", err)
	}
	if err := json.Unmarshal(b, &params); err != nil {
		return nil, fmt.Errorf("decoding JSON: %v", err)
	}

	return &params, nil
}

// saveChunkerParams saves the chunker params to the store.
func saveChunkerParams(ctx context.Context, s store.Store, bucket string, params *server.ChunkerParams) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	b, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encoding json: %v", err)
	}
	if err := s.Put(ctx, bucket, chunkParamsKey, bytes.NewReader(b)); err != nil {
		return fmt.Errorf("putting object to store: %v", err)
	}

	return nil
}

func getLoggerLevel(s string) zerolog.Level {
	switch s {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.WarnLevel
	}
}

var logger zerolog.Logger

func run() error {
	var serverConfig serverConfig
	flag.UintVar(&serverConfig.Port, "port", defaultPort, "server listening port")
	flag.StringVar(&serverConfig.Database, "db", defaultDatabase, "location of metadata cache")
	flag.BoolVar(&serverConfig.VersioningEnabled, "enable_versioning", false, "enable file versioning")
	flag.UintVar(&serverConfig.AvgChunkKiB, "chunk_size", defaultAvgKib, "average chunk size in KiB")
	flag.StringVar(&serverConfig.LogLevel, "log_level", defaultLogLevel, "server logging level")
	flag.StringVar(&serverConfig.TLSCert, "tls_cert", "", "server TLS certificate file")
	flag.StringVar(&serverConfig.TLSKey, "tls_key", "", "server TLS key file")
	flag.UintVar(&serverConfig.DLTimeoutMinutes, "download_timeout", defaultDLTimeoutMinutes, "the maximum allotted time, in minutes, for a client to download a file")

	var storeConfig storeConfig
	flag.StringVar(&storeConfig.AccessKey, "store_access_key", "", "access key for the object store")
	flag.StringVar(&storeConfig.SecretKey, "store_secret_key", "", "secret key for the object store")
	flag.StringVar(&storeConfig.Bucket, "store_bucket", "", "bucket name (required)")
	flag.BoolVar(&storeConfig.DisableSSL, "store_disable_ssl", false, "don't require an SSL connection to connect to the store")
	flag.BoolVar(&storeConfig.PathStyle, "store_path_style", false, "use path-style requests to the store")
	flag.StringVar(&storeConfig.Endpoint, "store_endpoint", "", "endpoint of S3-compatible store. Connects to AWS S3 by default")
	flag.StringVar(&storeConfig.Region, "store_region", "", "store region name")

	var debug bool
	var version bool
	flag.BoolVar(&debug, "debug", false, "enable debug output")
	flag.BoolVar(&version, "version", false, "output version info and exit")

	flag.Parse()

	if version {
		format := "%-10s:  %s\n"
		fmt.Printf(format, "Version", Version)
		fmt.Printf(format, "Build date", BuildDate)
		fmt.Printf(format, "Commit ID", CommitID)
		return nil
	}

	if err := serverConfig.validate(); err != nil {
		return err
	}
	if err := storeConfig.validate(); err != nil {
		return err
	}
	// Configure the logger
	if debug {
		logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger().Level(zerolog.DebugLevel)
		fmt.Println("Debug mode enabled")
	} else {
		level := getLoggerLevel(serverConfig.LogLevel)
		logger = zerolog.New(os.Stderr).With().Timestamp().Logger().Level(level)
		fmt.Printf("Logging level: %s\n", level.String())
	}

	adapter, err := openDB(serverConfig.Database)
	if err != nil {
		return fmt.Errorf("database: %v", err)
	}

	fmt.Printf("Connecting to object store %s\n", storeConfig.Endpoint)
	store, err := s3.New(s3.Config{
		Region:     storeConfig.Region,
		Endpoint:   storeConfig.Endpoint,
		AccessKey:  storeConfig.AccessKey,
		SecretKey:  storeConfig.SecretKey,
		PathStyle:  storeConfig.PathStyle,
		DisableSSL: storeConfig.DisableSSL,
	})
	if err != nil {
		return fmt.Errorf("connecting to store: ")
	}

	fmt.Printf("Using bucket %s\n", storeConfig.Bucket)

	// Get the chunking parameters from the store or create the object if it doesn't exist
	ctx := context.Background()
	chunkerParams, err := getChunkerParams(ctx, store, storeConfig.Bucket)
	if err != nil {
		return fmt.Errorf("getting chunker params: %v", err)
	}
	if chunkerParams == nil {
		avg := serverConfig.AvgChunkKiB * kiB
		chunkerParams = &server.ChunkerParams{
			MinChunkSize:  avg / 4,
			AvgChunkSize:  avg,
			MaxChunkSize:  avg * 4,
			Normalization: defaultNormalization,
		}
		if err = saveChunkerParams(ctx, store, storeConfig.Bucket, chunkerParams); err != nil {
			return fmt.Errorf("saving chunker params: %v", err)
		}
	}

	if serverConfig.VersioningEnabled {
		fmt.Println("File versioning enabled")
	} else {
		fmt.Println("File versioning disabled")
	}

	srv := server.New(adapter, store, server.Config{
		Bucket:            storeConfig.Bucket,
		VersioningEnabled: serverConfig.VersioningEnabled,
		MaxChunkSize:      uint64(chunkerParams.MaxChunkSize),
		MaxPackfileSize:   maxPackfileSize,
		DownloadTimeout:   time.Minute * time.Duration(serverConfig.DLTimeoutMinutes),
		Params:            *chunkerParams,
	})
	srv.SetLogger(logger)
	srvHandler := pb.NewJotFSServer(srv, loggingServerHooks())

	mux := http.NewServeMux()
	mux.Handle(srvHandler.PathPrefix(), srvHandler)
	mux.HandleFunc("/packfile", logHandler(postHandler(srv.PackfileUploadHandler), "PackfileUpload"))

	fmt.Printf("Listening on port %d\n", serverConfig.Port)
	if serverConfig.TLSCert != "" {
		fmt.Println("TLS enabled")
		return http.ListenAndServeTLS(fmt.Sprintf(":%d", serverConfig.Port), serverConfig.TLSCert, serverConfig.TLSKey, mux)
	} else {
		return http.ListenAndServe(fmt.Sprintf(":%d", serverConfig.Port), mux)
	}
}

// postHandler returns a http handler which returns a 500 error code unless invoked
// through a POST request.
func postHandler(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			code := http.StatusMethodNotAllowed
			http.Error(w, http.StatusText(code), code)
			return
		}
		handler(w, req)
	}
}

// logHandler returns a http handler which logs the status code and execution time of
// the request.
func logHandler(handler http.HandlerFunc, name string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		ww := &responseWriter{w, 0, ""}
		handler(ww, req)
		elapsedMillis := time.Since(start).Milliseconds()
		reqID := xid.New().String()

		w.Header().Set("x-jotfs-request-id", reqID)

		rpcLogger := logger.With().
			Str("method", name).
			Int("status", ww.statusCode).
			Int("elapsed", int(elapsedMillis)).
			Str("id", reqID).
			Logger()

		if 200 <= ww.statusCode && ww.statusCode < 300 {
			rpcLogger.Info().Msg("")
		} else if 400 <= ww.statusCode && ww.statusCode < 500 {
			rpcLogger.Warn().Msg(ww.errMsg)
		} else {
			rpcLogger.Error().Msg(ww.errMsg)
		}
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	errMsg     string
}

func (w *responseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.statusCode >= 400 {
		w.errMsg = string(p)
	}
	return w.ResponseWriter.Write(p)
}

func main() {
	err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}
