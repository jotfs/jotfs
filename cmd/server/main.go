package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/iotafs/iotafs/internal/db"
	twup "github.com/iotafs/iotafs/internal/protos/upload"
	"github.com/iotafs/iotafs/internal/server/upload"
	"github.com/iotafs/iotafs/internal/store/s3"
	"github.com/twitchtv/twirp"

	"github.com/BurntSushi/toml"
	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultDatabase = "./iotafs.db"
	defaultPort     = 6776
	defaultQSize    = 10

	defaultEndpoint = "s3.amazonaws.com"
	miB             = 1024 * 1024
)

type serverConfig struct {
	Port     int    `toml:"port"`
	Database string `toml:"database"`
}

type storeConfig struct {
	AccessKey  string `toml:"access_key"`
	SecretKey  string `toml:"secret_key"`
	Bucket     string `toml:"bucket"`
	Region     string `toml:"region"`
	DisableSSL bool   `toml:"disable_ssl"`
	PathStyle  bool   `toml:"path_style"`
	Endpoint   string `toml:"endpoint"`
}

type config struct {
	Server *serverConfig `toml:"server"`
	Store  *storeConfig  `toml:"store"`
}

func readConfig(filename string) (config, error) {
	if exists, err := fileExists(filename); err != nil {
		return config{}, err
	} else if !exists {
		return config{}, fmt.Errorf("config file %s not found", filename)
	}

	var cfg config
	if _, err := toml.DecodeFile(filename, &cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func openDB(filename string) (*db.Adapter, error) {
	exists, err := fileExists(filename)
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %v", filename, err)
	}
	if exists {
		log.Printf("Using existing database %s\n", filename)
	} else {
		log.Printf("Creating new database %s\n", filename)
	}
	sqldb, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", filename))
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

func requiredFieldError(field string) error {
	return fmt.Errorf("field %q is reqiured", field)
}

func (c serverConfig) validate() error {
	if c.Database == "" {
		return requiredFieldError("database")
	}
	return nil
}

func (c storeConfig) validate() error {
	if c.AccessKey == "" {
		return requiredFieldError("access_key")
	}
	if c.SecretKey == "" {
		return requiredFieldError("secret_key")
	}
	if c.Bucket == "" {
		return requiredFieldError("bucket")
	}
	return nil
}

func (c config) validate() error {
	if c.Server == nil {
		return fmt.Errorf("section [server] is required")
	}
	if c.Store == nil {
		return fmt.Errorf("section [store] is required")
	}
	if err := c.Server.validate(); err != nil {
		return fmt.Errorf("[server]: %w", err)
	}
	if err := c.Store.validate(); err != nil {
		return fmt.Errorf("[store]: %w", err)
	}
	return nil
}

func (c *serverConfig) setDefaults() {
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.Database == "" {
		c.Database = defaultDatabase
	}
}

func (c *storeConfig) setDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = defaultEndpoint
		log.Printf("Using default store endpoints %s\n", defaultEndpoint)
	}
}

func (c *config) setDefaults() {
	c.Server.setDefaults()
	c.Store.setDefaults()
}

func loggingServerHooks() *twirp.ServerHooks {
	hooks := &twirp.ServerHooks{}

	// Define a key type to keep context.WithValue happy
	type key int
	const receivedAtKey key = 1

	hooks.RequestReceived = func(ctx context.Context) (context.Context, error) {
		ctx = context.WithValue(ctx, receivedAtKey, time.Now())
		return ctx, nil
	}

	hooks.ResponseSent = func(ctx context.Context) {
		service, _ := twirp.ServiceName(ctx)
		method, _ := twirp.MethodName(ctx)
		code, _ := twirp.StatusCode(ctx)
		receivedAt, ok := ctx.Value("receivedAt").(time.Time)
		var timeMillis int64
		if ok {
			timeMillis = time.Now().Sub(receivedAt).Milliseconds()
		}
		log.Printf("%s %s.%s %dms", code, service, method, timeMillis)
	}

	return hooks
}

var configFileName = flag.String(
	"config",
	"iotafs.toml",
	"path to config file (default iotafs.toml)",
)

func run() error {
	flag.Parse()

	// Load and validate the config
	cfg, err := readConfig(*configFileName)
	if err != nil {
		return fmt.Errorf("reading config: %v", err)
	}
	if err := cfg.validate(); err != nil {
		return fmt.Errorf("invalid config: %v", err)
	}
	cfg.setDefaults()

	adapter, err := openDB(cfg.Server.Database)
	if err != nil {
		return fmt.Errorf("database: %v", err)
	}

	log.Printf("Connecting to store %s ... ", cfg.Store.Endpoint)
	store, err := s3.New(s3.Config{
		Region:     cfg.Store.Region,
		Endpoint:   cfg.Store.Endpoint,
		AccessKey:  cfg.Store.AccessKey,
		SecretKey:  cfg.Store.SecretKey,
		PathStyle:  cfg.Store.PathStyle,
		DisableSSL: cfg.Store.DisableSSL,
	})
	if err != nil {
		return fmt.Errorf("connecting to store: ")
	}
	log.Print("done")

	srv := upload.NewServer(adapter, store, upload.Config{
		Bucket:          cfg.Store.Bucket,
		MaxChunkSize:    8 * miB,
		MaxPackfileSize: 128 * miB,
	})
	srvHandler := twup.NewIotaFSServer(srv, loggingServerHooks())

	mux := http.NewServeMux()
	mux.Handle(srvHandler.PathPrefix(), srvHandler)
	mux.HandleFunc("/packfile", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" {
			code := http.StatusMethodNotAllowed
			http.Error(w, http.StatusText(code), code)
			return
		}
		srv.PackfileUploadHandler(w, req)
	})

	log.Printf("Listening on port %d", cfg.Server.Port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", cfg.Server.Port), mux)

	return err
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	os.Exit(0)
}
