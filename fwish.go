package fwish

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

//TODO: the DB's schemaID is the one with the highest authority. if the
// DB has it, the migrator and the source must provide matching schema ids.
//TODO: consider utilizing context.Context

// DB is an interface which can be fulfilled by a sql.DB instance.
// We have this abstraction so that people can use stdlib-compatible
// implementations, for example, github.com/jmoiron/sqlx .
//
type DB interface {
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
//TODO: interface so the source can lazy-checksum
type MigrationInfo struct {
	Name     string
	Script   string
	Checksum uint32
}

// MigrationSource is an abstraction for migration sources.
//
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
	metaTableName string
	validated     bool
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
//
//TODO: logger. two types: structured and unstructured. we should support
// both of them.
//TODO: make the instance safe for concurrent usage? for example we want
// to migrate multiple targets.
type Migrator struct {
	schemaID   string
	schemaName string //TODO: any actual use?
	userID     string

	sources    []MigrationSource
	migrations []migration     //TODO: list of {version, name, src} sorted by rank
	versions   map[string]bool // map to rank / index? to migration?

	logger LogOutputer
}

// NewMigrator creates a migrator instance with source loaded from
// folder pointed by sourceDir.
//
// The schemaID will be compared to the ID found inside the
// migration source meta file and the metadata table. The recommended
// value for schemaID is an UUID or the URI of the application.
//
func NewMigrator(schemaID string) (*Migrator, error) {
	m := &Migrator{
		schemaID: schemaID,
	}
	return m, nil
}

// WithLogger sets non-structured logger. It accepts Logger from Go's
// built-in package.
//
//TODO: logging level
func (m *Migrator) WithLogger(logger LogOutputer) *Migrator {
	m.logger = logger
	return m
}

// WithUserID sets the user identifier who performed the next migrations.
// Recommended value is user's email address.
//
func (m *Migrator) WithUserID(userID string) *Migrator {
	m.userID = userID
	return m
}

// AddSource register a migrations provider. The source must have the same
// schema ID as the migrator.
//
// All the migrations from all sources will be compiled.
//
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
		if id == "" || id != m.schemaID { // case-insensitive / case-fold?
			return ErrSchemaIDMismatch
		}
	} else {
		m.schemaID = id
	}

	ml, err := src.Migrations()
	if err != nil {
		return errors.Wrap(err, "fwish: unable to get source's migrations")
	}

	if m.versions == nil {
		m.versions = make(map[string]bool)
	}

	migrationVersionSeparator := "__"
	migrationVersionedPrefix := "V"

	for _, mi := range ml {
		//TODO: validate things!
		// version string is [0-9\.] with underscore deprecated
		mn := mi.Name
		//TODO: repeatable
		if !strings.HasPrefix(mn, migrationVersionedPrefix) {
			return errors.Errorf("fwish: migration name %q has invalid prefix", mn)
		}
		idx := strings.Index(mn, migrationVersionSeparator)
		if idx == -1 {
			//TODO: could we have name without the label part?
			return errors.Errorf("fwish: invalid migration name %q", mn)
		}
		vstr := mn[:idx]
		//TODO: proper label processing
		label := strings.TrimSpace(
			strings.Replace(
				mn[idx+len(migrationVersionSeparator):], "_", " ", -1))

		vstr = vstr[len(migrationVersionedPrefix):]
		if vstr == "" {
			return errors.Errorf("fwish: migration name %q has invalid version part", mn)
		}

		vstr, vints, err := m.parseVersion(vstr)
		if err != nil {
			return err
		}

		if _, ok := m.versions[vstr]; ok {
			//TODO: test case for this
			return errors.Errorf("fwish: duplicate version %q", vstr)
		}
		m.versions[vstr] = true

		m.migrations = append(m.migrations, migration{
			versionStr:  vstr,
			versionInts: vints,
			label:       label,
			name:        mn,
			script:      mi.Script,
			checksum:    mi.Checksum,
			source:      src,
		})
	}

	//TODO: test case for this
	sort.Slice(m.migrations, func(i, j int) bool {
		vlA := m.migrations[i].versionInts
		vlB := m.migrations[j].versionInts
		// There's at least one part
		if vlA[0] < vlB[0] {
			return true
		}
		var mx int
		if len(vlA) < len(vlB) {
			mx = len(vlA)
		} else {
			mx = len(vlB)
		}
		for k := 1; k < mx; k++ {
			if vlA[k] < vlB[k] {
				return true
			}
			if vlA[k] > vlB[k] {
				return false
			}
		}
		return len(vlA) < len(vlB)
	})

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
//TODO: allow override meta table name too?
//TODO: MigrateToRank, and MigrateToVersion ?
func (m *Migrator) Migrate(db DB, schemaName string) (num int, err error) {
	//TODO: validate the parameters
	// - we should use regex for schemaName. [A-Za-z0-9_]
	//TODO: use source's schemaName as the default?
	if schemaName == "" {
		schemaName = m.schemaName
	}
	if schemaName == "" {
		schemaName = "public"
	}
	st := &state{db, schemaName, "schema_version", false, -1}

	var searchPath string
	err = st.db.QueryRow("SHOW search_path").Scan(&searchPath)
	if err != nil {
		return 0, err
	}
	_, err = st.db.Exec("SET search_path TO " + st.schemaName)
	if err != nil {
		return 0, err
	}

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

	//TODO: use Tx
	for i := int(st.installedRank); i < len(m.migrations); i++ {
		sf := m.migrations[i]
		if m.logger != nil {
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

	_, err = st.db.Exec("SET search_path TO " + searchPath)
	return num, err
}

// Status returns whether all the migrations have been applied.
//
func (m *Migrator) Status(db DB) (diff int, err error) {
	// if err := m.ensureSourceFilesScanned(); err != nil {
	// 	return 0, err
	// }

	// SELECT * FROM table WHERE status IS TRUE

	return 0, errors.New("not implemented yet")
}

func (m *Migrator) ensureDBSchemaInitialized(st *state) error {
	// if st.schemaInit {
	// 	return nil
	// }
	// st.schemaInit = true

	//TODO: all these things should be inside a transaction
	//TODO: if the meta table does not exist or there's no revision but
	// the schema already has other tables, we should return error.
	//TODO: if the DB has no schema meta but already has entries,
	// we assume that it's a from fw. if the migrator has valid
	// schemaID, set the meta, otherwise we don't bother with schemaID.

	// IF NOT EXISTS is available starting from 9.3 (TODO: get postgres'
	// version; we'll need it to limit our support anyway)
	// Let's try to create the schema away.
	_, err := st.db.Exec(fmt.Sprintf(
		`CREATE SCHEMA %s`,
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

	var idstr string

	err = st.db.QueryRow(fmt.Sprintf(
		`SELECT script FROM %s.%s WHERE installed_rank=0`,
		st.schemaName, st.metaTableName,
	)).Scan(&idstr)
	if err == nil {
		if idstr != m.schemaID {
			return ErrSchemaIDMismatch
		}
		return nil
	}

	if err != sql.ErrNoRows {
		pqErr, ok := err.(*pq.Error)
		if !ok {
			return errors.Wrap(err, "fwish: unexpected error type")
		}

		// 42P01: undefined_table
		if pqErr.Code != "42P01" ||
			!strings.Contains(pqErr.Message, `"`+st.schemaName+`.`+st.metaTableName+`"`) {
			return fmt.Errorf("fwish: unexpected error (%v)", pqErr)
		}

		_, err = st.db.Exec(fmt.Sprintf(
			`CREATE TABLE %s.%s (
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
			st.schemaName, st.metaTableName, st.metaTableName,
		))
		if err != nil {
			return err
		}
	}

	//TODO: ensure indexes

	_, err = st.db.Exec(
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
			st.schemaName, st.metaTableName,
		),
		"0", st.schemaName, m.schemaID, m.userID, time.Now().UTC(),
	)
	if err != nil {
		return err
	}

	st.installedRank = 0

	return nil
}

func (m *Migrator) validateDBSchema(st *state) error {
	st.validated = false //
	st.installedRank = -1

	//TODO: lazy-load source migration checksums

	rows, err := st.db.Query(fmt.Sprintf(
		`SELECT installed_rank, version, script, checksum, success
		FROM %s.%s ORDER BY installed_rank`,
		st.schemaName, st.metaTableName,
	))
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if !ok {
			return err
		}
		// 42P01: undefined_table
		if pqErr.Code != "42P01" ||
			!strings.Contains(pqErr.Message, `"`+st.schemaName+`.`+st.metaTableName+`"`) {
			return err
		}
		// Set the status as validated eventhough the schema has not been
		// initialized. Use installedRank to check if the schema has been
		// initialized (>= 0 means initialized)
		st.validated = true
		return nil
	}

	var i, rank int32
	var version, script string
	var checksum uint32
	var success bool

	for i = 0; rows.Next(); i++ {
		err = rows.Scan(&rank, &version, &script, &checksum, &success)
		if err != nil {
			return err
		}
		//TODO: validate
		if rank != i {
			// class: schema consistency
			return errors.New("fwish: insequential installed_rank")
		}

		if !success {
			// what to do?
			panic("DB has failed migration")
		}

		if int(i) > len(m.migrations) {
			//TODO: a test for this case
			return errors.New("fwish: DB has more migrations than the source")
		}

		if i == 0 {
			continue
		}

		mig := m.migrations[i-1]

		if mig.checksum != checksum {
			//TODO: ensure the message has enough details
			return errors.Errorf("fwish: checksum mismatch for rank %d: %s", i, script)
		}

		// check other stuff?

		st.installedRank = i
	}

	return rows.Err()
}

func (m *Migrator) executeMigration(st *state, rank int32, sf *migration) error {
	tStart := time.Now()

	err := sf.source.ExecuteMigration(st.db, MigrationInfo{
		Name:     sf.name,
		Script:   sf.script,
		Checksum: sf.checksum,
	})
	if err != nil {
		return err
	}

	dt := time.Since(tStart) / time.Millisecond

	_, err = st.db.Exec(
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
			VALUES ($1,$2,$3,'SQL',$4,$5,$6,$7,$8,true)`,
			st.schemaName, st.metaTableName,
		),
		rank, sf.versionStr, sf.label, sf.script, sf.checksum,
		m.userID, tStart.UTC(), dt,
	)

	return err
}

func (m *Migrator) parseVersion(vstr string) (normalized string, parts []int64, err error) {
	//TODO: we might want to support underscore for compatibility.
	// some source might using class name for the migration name.
	pl := strings.Split(vstr, ".")
	if len(pl) == 1 {
		// Try underscore
		pl = strings.Split(vstr, "_")
	}
	vints := make([]int64, len(pl))
	for i, sv := range pl {
		// note that we don't need to trim left zeroes as we explicitly
		// tell the parser that the number is a decimal.
		iv, err := strconv.ParseInt(sv, 10, 64)
		if err != nil {
			return "", nil, errors.Errorf("fwish: version %q contains invalid value", vstr)
		}
		vints[i] = iv
	}
	// Convert them back to string
	sl := make([]string, len(vints))
	for i, iv := range vints {
		sl[i] = strconv.FormatInt(iv, 10)
	}
	return strings.Join(sl, "."), vints, nil
}
