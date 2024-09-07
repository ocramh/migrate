package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	nurl "net/url"
	"strconv"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/hashicorp/go-multierror"
	uatomic "go.uber.org/atomic"
	"google.golang.org/api/iterator"
	bqopt "google.golang.org/api/option"
)

func init() {
	db := BigQuery{}
	database.Register("bigquery", &db)
}

const (
	DefaultMigrationsTableName = "schema_migrations"
	DefaultStmtTimeoutSec      = 10
)

var (
	dbUnlocked = false
	dbLocked   = true

	ErrDBLocked         = errors.New("unable to obtain database lock")
	ErrDBNotLocked      = errors.New("database already unlocked")
	ErrMissingConfigArg = errors.New("config is a required argument")
)

type BigQuery struct {
	DB     *bq.Client
	config *Config
	lock   *uatomic.Bool
}

type Config struct {
	MigrationsTable string
	DatasetID       string
	GCPProjectID    string
	StmtTimeout     time.Duration
}

// WithInstance is an optional function that accepts an existing DB instance, a
// Config struct and returns a driver instance.
func WithInstance(instance *bq.Client, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrMissingConfigArg
	}

	b := &BigQuery{
		DB:     instance,
		config: config,
		lock:   uatomic.NewBool(dbUnlocked),
	}

	err := b.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Open returns a new driver instance configured with parameters
// coming from the URL string. Migrate will call this function
// only once per instance.
func (b *BigQuery) Open(url string) (database.Driver, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	projectID, datasetID, err := parseDNS(u.String())
	if err != nil {
		return nil, err
	}

	migrationsTable := DefaultMigrationsTableName
	if u.Query().Has("x-migrations-table") {
		migrationsTable = u.Query().Get("x-migrations-table")
	}

	stmtTimeout := DefaultStmtTimeoutSec
	if u.Query().Has("x-stmt-timeout") {
		stmtTimeout, err = strconv.Atoi(u.Query().Get("x-stmt-timeout"))
		if err != nil {
			return nil, err
		}
	}

	opt := []bqopt.ClientOption{}

	ctx := context.Background()
	client, err := bq.NewClient(ctx, projectID, opt...)
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		DB: client,
		config: &Config{
			MigrationsTable: migrationsTable,
			DatasetID:       datasetID,
			GCPProjectID:    projectID,
			StmtTimeout:     time.Duration(stmtTimeout),
		},
		lock: uatomic.NewBool(dbUnlocked),
	}, nil
}

// Close closes the underlying database instance managed by the driver.
// Migrate will call this function only once per instance.
func (b *BigQuery) Close() error {
	return b.DB.Close()
}

// Lock should acquire a database lock so that only one migration process
// can run at a time. Migrate will call this function before Run is called.
// If the implementation can't provide this functionality, return nil.
// Return database.ErrLocked if database is already locked.
func (b *BigQuery) Lock() error {
	if isLocked := b.lock.CAS(dbUnlocked, dbLocked); isLocked {
		return nil
	}

	return ErrDBLocked
}

// Unlock should release the lock. Migrate will call this function after
// all migrations have been run.
func (b *BigQuery) Unlock() error {
	if isUnlocked := b.lock.CAS(dbLocked, dbUnlocked); isUnlocked {
		return nil
	}

	return ErrDBNotLocked
}

// Run applies a migration to the database. migration is guaranteed to be not nil.
func (b *BigQuery) Run(migration io.Reader) error {
	stmt, err := io.ReadAll(migration)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(string(stmt))

	job, err := query.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	return status.Err()
}

// SetVersion saves version and dirty state.
// Migrate will call this function before and after each call to Run.
// version must be >= -1. -1 means NilVersion.
func (b *BigQuery) SetVersion(version int, dirty bool) error {
	stmt := fmt.Sprintf("INSERT INTO `%s` (version, dirty) VALUES (@version, @dirty)",
		b.config.MigrationsTable,
	)

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(string(stmt))
	query.Parameters = []bq.QueryParameter{
		{Name: "version", Value: version},
		{Name: "dirty", Value: dirty},
	}

	job, err := query.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	return status.Err()
}

// Version returns the currently active version and if the database is dirty.
// When no migration has been applied, it must return version -1.
// Dirty means, a previous migration failed and user interaction is required.
func (b *BigQuery) Version() (version int, dirty bool, err error) {
	stmt := fmt.Sprintf("SELECT version, dirty FROM `%s` ORDER BY version DESC LIMIT 1",
		b.config.MigrationsTable,
	)

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	query := b.DB.Query(string(stmt))

	job, err := query.Run(ctx)
	if err != nil {
		return version, dirty, err
	}

	rowIter, err := job.Read(ctx)
	if err != nil {
		return version, dirty, err
	}

	type versionRow struct {
		Version int  `bigquery:"version"`
		Dirty   bool `bigquery:"dirty"`
	}
	v := versionRow{}

	err = rowIter.Next(&v)
	if err != nil {
		return version, dirty, err
	}

	return v.Version, v.Dirty, nil
}

// Drop deletes everything in the database.
func (b *BigQuery) Drop() error {
	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	tablesIter := b.DB.Dataset(b.config.DatasetID).Tables(ctx)

	for {
		table, err := tablesIter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return err
		}

		err = table.Delete(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// ensureVersionTable creates the migrations version table if it doesn't already
// exist
func (b *BigQuery) ensureVersionTable() (err error) {
	err = b.Lock()
	if err != nil {
		return err
	}

	defer func() {
		unlockErr := b.Unlock()
		if unlockErr != nil {
			if err != nil {
				err = multierror.Append(err, unlockErr)
			}

			err = unlockErr
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), b.config.StmtTimeout)
	defer cancel()

	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version INT64 NOT NULL,
    dirty    BOOL NOT NULL
	)`, b.config.MigrationsTable)

	q := b.DB.Query(stmt)

	job, err := q.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	return status.Err()
}

// parseDNS returns the projectID and datasetID fragments from a URL connection
// string like bigquery://{projectID}/{datasetID}?param=true
func parseDNS(url string) (string, string, error) {
	url = strings.Replace(url, "bigquery://", "", 1)
	fragments := strings.Split(url, "/")

	if len(fragments) != 2 {
		return "", "", errors.New("invalid url format expected, bigquery://{projectID}/{datasetID}?param=true")
	}

	return fragments[0], fragments[1], nil
}
