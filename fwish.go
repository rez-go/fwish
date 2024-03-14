package fwish

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/rez-go/fwish/version"
)

const (
	SchemaNameDefault    = "public"
	MetatableNameDefault = "schema_version"
)

//TODO: the DB's schemaID is the one with the highest authority. if the
// DB has it, the migrator and the source must provide matching schema ids.
//TODO: consider utilizing context.Context
//TODO: version assertion

// DB is an interface which can be fulfilled by a sql.DB instance.
// We have this abstraction so that people can use stdlib-compatible
// implementations, for example, github.com/jmoiron/sqlx .
type DB interface {
	Begin() (*sql.Tx, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// LogOutputer is an interface for non-structured logger. This
// interface is compatible with stdlib's Logger.
type LogOutputer interface {
	Output(calldepth int, s string) error
}

// MigrationInfo holds basic info about a migration obtained from
// a source.
//
// TODO: interface so the source can lazy-checksum
type MigrationInfo struct {
	Name     string
	Script   string
	Checksum uint32
}

// MigrationSource is an abstraction for migration sources.
type MigrationSource interface {
	SchemaID() string
	SchemaName() string
	Migrations() ([]MigrationInfo, error)
	ExecuteMigration(db DB, migration MigrationInfo) error //TODO: use context and Tx
}

var (
	// ErrSchemaIDMismatch is returned when the provided ID doesn't match
	// schema's ID.
	ErrSchemaIDMismatch = errors.New("fwish: schema ID mismatch")
)

// Might want store the tx in here too
type state struct {
	db            DB
	schemaName    string
	metatableName string
	installedRank int32
}

type migration struct {
	versionStr  string
	versionInts []int64
	label       string
	name        string
	script      string
	checksum    uint32
	source      MigrationSource
}

// Migrator is the ..
type Migrator struct {
	schemaID   string
	schemaName string
	userID     string

	sources    []MigrationSource
	versions   []string
	migrations map[string]migration

	logger LogOutputer
}

// NewMigrator creates a migrator instance with source loaded from
// folder pointed by sourceDir.
//
// The schemaID will be compared to the ID found inside the
// migration source meta file and the metadata table. The recommended
// value for schemaID is an UUID or the URI of the application.
func NewMigrator(schemaID string) (*Migrator, error) {
	m := &Migrator{
		schemaID: schemaID,
	}
	return m, nil
}

// WithLogger sets non-structured logger. It accepts Logger from Go's
// built-in package.
func (m *Migrator) WithLogger(logger LogOutputer) *Migrator {
	m.logger = logger
	return m
}

// WithUserID sets the user identifier who performed the next migrations.
// Recommended value is user's email address.
func (m *Migrator) WithUserID(userID string) *Migrator {
	m.userID = userID
	return m
}

// AddSource register a migrations provider. The source must have the same
// schema ID as the migrator.
//
// All the migrations from all sources will be compiled.
func (m *Migrator) AddSource(src MigrationSource) error {
	//TODO: currently, if we failed while adding migration, the state
	// of migrator is undefined, we should prevent undefined state.
	// we first validate all the migrations first then apply the
	// apply the changes after all have been validated.

	//TODO: get the schemaName from the source with first rank
	if m.schemaName == "" {
		m.schemaName = src.SchemaName()
		//TODO: ensure valid schema name
	}
	id := src.SchemaID()
	if m.schemaID != "" {
		if id == "" || id != m.schemaID {
			return ErrSchemaIDMismatch
		}
	} else {
		m.schemaID = id
	}

	ml, err := src.Migrations()
	if err != nil {
		return fmt.Errorf("fwish: unable to get source's migrations: %w", err)
	}

	if m.migrations == nil {
		m.migrations = make(map[string]migration)
	}

	migrationVersionSeparator := "__"
	migrationVersionedPrefix := "V"

	for _, mi := range ml {
		//TODO: validate things!
		// version string is [0-9\.]
		mn := mi.Name
		//TODO: support for repeatables
		if !strings.HasPrefix(mn, migrationVersionedPrefix) {
			return fmt.Errorf("fwish: migration name %q has invalid prefix", mn)
		}
		idx := strings.Index(mn, migrationVersionSeparator)
		if idx == -1 {
			return fmt.Errorf("fwish: invalid migration name %q", mn)
		}
		vstr := mn[:idx]
		label := strings.TrimSpace(
			strings.Replace(
				mn[idx+len(migrationVersionSeparator):], "_", " ", -1))

		vstr = vstr[len(migrationVersionedPrefix):]
		if vstr == "" {
			return fmt.Errorf("fwish: migration name %q has invalid version part", mn)
		}

		vints, err := version.Parse(vstr)
		if err != nil {
			return err
		}
		vstr = vints.String()
		if vstr == "" {
			// This would be an internal error
			return fmt.Errorf("fwish: migration %q has empty version", mn)
		}

		if cv, ok := m.migrations[vstr]; ok {
			//TODO: test case for this
			return fmt.Errorf("fwish: version %q conflict (%q, %q)", vstr, cv.name, mn)
		}
		m.migrations[vstr] = migration{
			versionStr:  vstr,
			versionInts: vints,
			label:       label,
			name:        mn,
			script:      mi.Script,
			checksum:    mi.Checksum,
			source:      src,
		}
		m.versions = append(m.versions, vstr)
	}

	err = version.SortStrings(m.versions)
	if err != nil {
		return err
	}

	m.sources = append(m.sources, src)

	return nil
}

// SchemaID returns the ID of the schema the migrations are for.
func (m *Migrator) SchemaID() string { return m.schemaID }

// Migrate execute the migrations.
//
// The schemaName parameter will be used to override the schema name
// found inside the meta file. The schema name corresponds the
// Postgres database schema name.
//
// TODO: allow override meta table name too?
// TODO: MigrateToRank, and MigrateToVersion ?
func (m *Migrator) Migrate(db DB, schemaName string) (num int, err error) {
	//TODO: validate the parameters
	// - we should use regex for schemaName. [A-Za-z0-9_]
	//TODO: use source's schemaName as the default?
	if schemaName == "" {
		schemaName = m.schemaName
	}
	if schemaName == "" {
		schemaName = SchemaNameDefault
	}
	st := &state{db, schemaName, MetatableNameDefault, -1}

	var searchPath string
	err = st.db.QueryRow("SHOW search_path").Scan(&searchPath)
	if err != nil {
		return 0, err
	}

	_, err = st.db.Exec("SET search_path TO " + st.schemaName)
	if err != nil {
		return 0, err
	}
	defer func() {
		_, err = st.db.Exec("SET search_path TO " + searchPath)
		if err != nil {
			if logger := m.logger; logger != nil {
				logger.Output(2, "SET search_path returned error: "+err.Error())
			}
		}
	}()

	err = m.validateDBSchema(st)
	if err != nil {
		return 0, err
	}

	if st.installedRank == -1 {
		err = m.ensureDBSchemaInitialized(st)
		if err != nil {
			return 0, err
		}
	}

	// All in a Tx?
	for i := int(st.installedRank); i < len(m.versions); i++ {
		sf := m.migrations[m.versions[i]]
		if m.logger != nil {
			// nolint: errcheck
			m.logger.Output(2, fmt.Sprintf(
				"Migrating schema %q to version %s - %s",
				st.schemaName, sf.versionStr, sf.label,
			))
		}
		err = m.executeMigration(st, int32(i+1), &sf)
		if err != nil {
			return 0, err
		}
		num++
	}

	return num, nil
}

// Status returns whether all the migrations have been applied.
//
// TODO: it should also report if there's any failing migration.
func (m *Migrator) Status(db DB) (diff int, err error) {
	// if err := m.ensureSourceFilesScanned(); err != nil {
	// 	return 0, err
	// }

	// SELECT * FROM table WHERE status IS TRUE

	return 0, errors.New("not implemented yet")
}

func (m *Migrator) ensureDBSchemaInitialized(st *state) error {
	err := doTx(st.db, func(tx *sql.Tx) error {
		//TODO: if the meta table does not exist or there's no revision but
		// the schema already has other tables, we should return error.
		//TODO: if the DB has no schema meta but already has entries,
		// we assume that it's a from fw. if the migrator has valid
		// schemaID, set the meta, otherwise we don't bother with schemaID.

		_, err := tx.Exec(fmt.Sprintf(
			`CREATE SCHEMA IF NOT EXISTS %s`,
			st.schemaName,
		))
		if err != nil {
			pqErr, ok := err.(*pq.Error)
			if !ok {
				return err
			}
			if pqErr.Code != "42P06" || !strings.Contains(pqErr.Message,
				`"`+st.schemaName+`"`) {
				return pqErr
			}
		}

		_, err = tx.Exec(fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s (
			installed_rank integer NOT NULL,
			version character varying(50),
			description character varying(200) NOT NULL,
			type character varying(20) NOT NULL,
			script character varying(1000) NOT NULL,
			checksum integer,
			installed_by character varying(100) NOT NULL,
			installed_on timestamp without time zone NOT NULL DEFAULT now(),
			execution_time integer NOT NULL,
			success boolean NOT NULL,
			CONSTRAINT %s_pk PRIMARY KEY (installed_rank)
		)`,
			st.schemaName, st.metatableName, st.metatableName,
		))
		if err != nil {
			return err
		}

		var idstr string

		err = tx.QueryRow(fmt.Sprintf(
			`SELECT script FROM %s.%s WHERE installed_rank=0`,
			st.schemaName, st.metatableName,
		)).Scan(&idstr)
		if err == nil {
			if idstr != m.schemaID {
				return ErrSchemaIDMismatch
			}
			return nil
		}

		if err != sql.ErrNoRows {
			return err
		}

		//TODO: ensure indexes

		_, err = tx.Exec(
			fmt.Sprintf(
				`INSERT INTO %s.%s (
				installed_rank,
				version,
				description,
				type,
				script,
				checksum,
				installed_by,
				installed_on,
				execution_time,
				success )
			VALUES (0,$1,$2,'meta',$3,0,$4,$5,0,true)`,
				st.schemaName, st.metatableName,
			),
			"0", st.schemaName, m.schemaID, m.userID, time.Now().UTC(),
		)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	st.installedRank = 0

	return nil
}

func (m *Migrator) validateDBSchema(st *state) error {
	st.installedRank = -1

	//TODO: lazy-load source migration checksums

	rows, err := st.db.Query(fmt.Sprintf(
		`SELECT installed_rank, version, script, checksum, success
		FROM %s.%s ORDER BY installed_rank`,
		st.schemaName, st.metatableName,
	))
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if !ok {
			return err
		}
		// 42P01: undefined_table
		if pqErr.Code != "42P01" ||
			!strings.Contains(pqErr.Message, `"`+st.schemaName+`.`+st.metatableName+`"`) {
			return err
		}
		return nil
	}

	var i, rank int32
	var version, script string
	var checksum int32
	var success bool

	for i = 0; rows.Next(); i++ {
		err = rows.Scan(&rank, &version, &script, &checksum, &success)
		if err != nil {
			return err
		}

		//TODO: validate things

		if rank != i {
			// class: schema consistency
			return errors.New("fwish: insequential installed_rank")
		}

		if !success {
			// what to do?
			panic("DB has failed migration")
		}

		if int(i) > len(m.versions) {
			//TODO: a test for this case
			return errors.New("fwish: DB has more migrations than the source")
		}

		if i == 0 {
			continue
		}

		mig := m.migrations[m.versions[i-1]]

		if mig.checksum != uint32(checksum) {
			return fmt.Errorf("fwish: checksum mismatch for rank %d: %s", i, script)
		}

		// check other stuff?

		st.installedRank = i
	}

	return rows.Err()
}

func (m *Migrator) executeMigration(st *state, rank int32, sf *migration) error {
	tStart := time.Now()

	// Insert the row first but with success flag set as false. This is
	// so that we will know when a migration has failed.
	_, err := st.db.Exec(
		fmt.Sprintf(
			`INSERT INTO %s.%s (
				installed_rank,
				version,
				description,
				type,
				script,
				checksum,
				installed_by,
				installed_on,
				execution_time,
				success )
			VALUES ($1,$2,$3,'SQL',$4,$5,$6,$7,$8,false)`,
			st.schemaName, st.metatableName,
		),
		rank, sf.versionStr, sf.label, sf.script, int32(sf.checksum),
		m.userID, tStart.UTC(), 0,
	)
	if err != nil {
		return err
	}

	err = sf.source.ExecuteMigration(st.db, MigrationInfo{
		Name:     sf.name,
		Script:   sf.script,
		Checksum: sf.checksum,
	})
	if err != nil {
		return err
	}

	dt := time.Since(tStart) / time.Millisecond

	// Update the row to indicate that it's was a success.
	_, err = st.db.Exec(
		fmt.Sprintf(
			`UPDATE %s.%s
				SET (
					execution_time,
					success )
				= ($1,true)
				WHERE installed_rank=$2 AND success IS FALSE`,
			st.schemaName, st.metatableName,
		),
		dt, rank,
	)

	return err
}

func doTx(db DB, txFunc func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			tx.Rollback()
			panic(rec)
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	err = txFunc(tx)
	return err
}
