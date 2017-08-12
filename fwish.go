package fwish

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// DB is an interface which can be fulfilled by a sql.DB instance.
// We have this abstraction so that people can use stdlib-compatible
// implementations, for example, github.com/jmoiron/sqlx .
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

//TODO: interface so the source can lazy-checksum
type SourceMigration struct {
	Name     string
	Checksum uint32
}

type Source interface {
	SchemaID() string
	SchemaName() string
	Migrations() ([]SourceMigration, error)
	ExecuteMigration(db DB, migration SourceMigration) error //TODO: use context and Tx
}

var (
	// ErrSchemaIDMismatch is returned when the provided ID doesn't match
	// schema's ID.
	ErrSchemaIDMismatch = errors.New("fwish: schema ID mismatch")
)

// Might want store the tx in here too
type state struct {
	schemaName    string
	db            DB
	validated     bool
	installedRank int32
}

type migration struct {
	version  string // normalized for easy comparison?
	label    string
	name     string
	checksum uint32
	source   Source
}

// Migrator is the ..
//
//TODO: logger. two types: structured and unstructured. we should support
// both of them.
//TODO: make safe for concurrent usage?
type Migrator struct {
	schemaName    string //TODO: any actual use?
	schemaID      string
	metaTableName string

	migrations []migration     //TODO: list of {version, name, src} sorted by rank
	versions   map[string]bool // map to rank / index? to migration?
	sources    []Source

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
	schemaID = strings.TrimSpace(schemaID)

	m := &Migrator{
		schemaID:      schemaID,
		metaTableName: "schema_version",
	}

	//TODO: inspect the source files
	//TODO: lazy load. don't load the source before we receive
	// the command.

	return m, nil
}

//TODO: logging level
func (m *Migrator) WithLogger(logger LogOutputer) *Migrator {
	m.logger = logger
	return m
}

// AddSource register a migrations provider. The source must have the same
// schema ID as the migrator.
func (m *Migrator) AddSource(src Source) error {
	//TODO: everytime this function is called, we inspect the new source
	// and get info about all migrations it contained and insert those
	// migrations into our master list based on the ranks.

	//TODO: get the schemaName from the source with first rank
	if m.schemaName == "" {
		m.schemaName = src.SchemaName()
		//TODO: ensure valid schema name
	}
	if m.schemaName == "" {
		//TODO: move somewhere more appropriate or if it's empty,
		// use default, e.g., "public"
		//TODO: more descriptive error message
		return errors.New("fwish: undefined schema name")
	}
	id := src.SchemaID()
	if id == "" || id != m.schemaID { // case-insensitive / case-fold?
		return ErrSchemaIDMismatch
	}

	ml, err := src.Migrations()
	if err != nil {
		return errors.Wrap(err, "fwish: unable to get source's migration names")
	}

	if m.versions == nil {
		m.versions = make(map[string]bool)
	}

	//TODO: dupe
	suffix := ".sql"
	versionSep := "__"

	for _, mi := range ml {
		//TODO: validate things!
		// version string is [0-9\.] with underscore deprecated
		mn := mi.Name
		idx := strings.Index(mn, versionSep)
		if idx == -1 {
			return errors.Errorf("fwish: invalid migration name %q", mn)
		}
		vstr := mn[:idx]
		//TODO: replace the underscore with space and do other stuff
		label := strings.TrimSpace(
			strings.Replace(
				mn[idx+len(versionSep):len(mn)-len(suffix)], "_", " ", -1))

		if _, ok := m.versions[vstr]; ok {
			//TODO: test case for this
			return errors.Errorf("fwish: duplicate version %q", vstr)
		}

		m.migrations = append(m.migrations, migration{
			version:  vstr,
			label:    label,
			name:     mn,
			checksum: mi.Checksum,
			source:   src,
		})
	}

	//NOTE: this won't work intuitively
	// try sorting
	// "V1", "V100", "V1.0", "V1.2", "V1.3-test", "V2", "V10", "V001", "V002", "V200"
	//TODO: use numeric comparison
	sort.Slice(m.migrations, func(i, j int) bool {
		return strings.Compare(m.migrations[i].version, m.migrations[j].version) < 0
	})

	m.sources = append(m.sources, src)

	return nil
}

// SchemaName returns the name of the schema the migrations are for.
func (m *Migrator) SchemaName() string { return m.schemaName }

// Migrate execute the migrations.
//
// The schemaName parameter will be used to override the schema name
// found inside the meta file. The schema name corresponds the
// Postgres database schema name.
//
//TODO: parameter might be something {DB, schemaName}
//TODO: allow override meta table name too?
//TODO: MigrateToRank, and MigrateToVersion ?
func (m *Migrator) Migrate(db DB, schemaName string) (num int, err error) {
	//TODO: validate the parameters
	// - we should use regex for schemaName. [A-Za-z0-9_]
	//TODO: use source's schemaName as the default?
	st := &state{schemaName, db, false, -1}

	var searchPath string
	//TODO: get search path, set into specified schema, execute the
	// migrations, restore the search path.
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
				st.schemaName, sf.version, sf.label,
			))
		}
		err = m.applySourceFile(st, int32(i+1), &sf)
		if err != nil {
			return 0, err
		}
		num++
	}

	_, err = st.db.Exec("SET search_path TO " + searchPath)
	if err != nil {
		return num, err
	}

	return num, nil
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

// func (m *Migrator) ensureSourceFilesScanned() error {
// 	if m.source.scanned {
// 		return nil
// 	}
// 	_, err := m.scanSourceDir()
// 	return err
// }

// returns the number of files?
// func (m *Migrator) scanSourceDir() (numFiles int, err error) {
// 	// We might want to make these configurable
// 	ignorePrefix := "_"
// 	suffix := ".sql"
// 	versionSep := "__"

// 	numFiles = 0
// 	m.source.files = make(map[string]sourceFile)
// 	m.source.sortedFilenames = nil

// 	fl, err := ioutil.ReadDir(m.source.url)
// 	if err != nil {
// 		return 0, err //TODO: handle no such file (and wrap the error)
// 	}

// 	for _, entry := range fl {
// 		fname := entry.Name()

// 		if strings.HasPrefix(fname, ignorePrefix) {
// 			continue
// 		}
// 		if !strings.HasSuffix(fname, suffix) {
// 			continue
// 		}

// 		if idx := strings.Index(fname, versionSep); idx > 0 {
// 			vstr := fname[:idx]
// 			//TODO: replace the underscore with space and do other stuff
// 			label := fname[idx+len(versionSep) : len(fname)-len(suffix)]

// 			//TODO: get the first line of comment as the desc
// 			cksum, err := m.checksumSourceFile(filepath.Join(m.source.url, fname))
// 			if err != nil {
// 				panic(err)
// 			}
// 			if cksum == 0 {
// 				//TODO: check the reference behavior
// 				continue
// 			}

// 			//TODO: inspect the file?
// 			//TODO: ensure no files with the same version
// 			if _, ok := m.source.files[vstr]; ok {
// 				//TODO: write test case for this
// 				return 0, errors.Errorf("fwish: version %q duplicated", vstr)
// 			}

// 			m.source.files[vstr] = sourceFile{
// 				version:  vstr,
// 				label:    label,
// 				filename: fname,
// 				checksum: cksum,
// 			}
// 			m.source.sortedFilenames = append(m.source.sortedFilenames, vstr)
// 		}
// 	}

// 	//NOTE: this won't work intuitively
// 	// try sorting
// 	// "V1", "V100", "V1.0", "V1.2", "V1.3-test", "V2", "V10", "V001", "V002", "V200"
// 	sort.Strings(m.source.sortedFilenames)

// 	m.source.scanned = true

// 	return len(m.source.sortedFilenames), nil
// }

// func (m *Migrator) checksumSourceFile(filename string) (uint32, error) {
// 	fh, err := os.Open(filename)
// 	if err != nil {
// 		return 0, errors.Wrap(err, "fwish: unable opening file for checksum")
// 	}
// 	defer fh.Close()

// 	scanner := bufio.NewScanner(fh)
// 	scanner.Split(bufio.ScanLines)

// 	ck := crc32.NewIEEE()
// 	for scanner.Scan() {
// 		ck.Write(scanner.Bytes())
// 	}

// 	return ck.Sum32(), nil
// }

//TODO: if the meta table does not exist or there's no revision but the schema already
// has other tables, we should return error.
func (m *Migrator) ensureDBSchemaInitialized(st *state) error {
	// if st.schemaInit {
	// 	return nil
	// }
	// st.schemaInit = true

	//TODO: all these things should be inside a transaction

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

	//TODO: if the table is not empty, and there's no meta, should we
	// return an error?
	err = st.db.QueryRow(fmt.Sprintf(
		`SELECT script FROM %s.%s WHERE installed_rank=0`,
		st.schemaName, m.metaTableName,
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
			!strings.Contains(pqErr.Message, `"`+st.schemaName+`.`+m.metaTableName+`"`) {
			return fmt.Errorf("fwish: unexpected error (%v)", pqErr)
		}

		_, err := st.db.Exec(fmt.Sprintf(
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
			st.schemaName, m.metaTableName, m.metaTableName,
		))
		if err != nil {
			return err
		}
	}

	//TODO: ensure indexes

	//TODO: set desc and installed_by
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
			st.schemaName, m.metaTableName,
		),
		"0", st.schemaName, m.schemaID, "", time.Now().UTC(),
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

	// if err := m.ensureSourceFilesScanned(); err != nil {
	// 	return err
	// }

	rows, err := st.db.Query(fmt.Sprintf(
		`SELECT installed_rank, version, script, checksum, success
		FROM %s.%s ORDER BY installed_rank`,
		st.schemaName, m.metaTableName,
	))
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if !ok {
			return err
		}
		// 42P01: undefined_table
		if pqErr.Code != "42P01" ||
			!strings.Contains(pqErr.Message, `"`+st.schemaName+`.`+m.metaTableName+`"`) {
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
	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}

func (m *Migrator) applySourceFile(st *state, rank int32, sf *migration) error {
	tStart := time.Now()

	err := sf.source.ExecuteMigration(st.db, SourceMigration{
		Name:     sf.name,
		Checksum: sf.checksum,
	})
	if err != nil {
		return err
	}

	dt := time.Now().Sub(tStart) / time.Millisecond

	//TODO: set desc and installed_by
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
			st.schemaName, m.metaTableName,
		),
		rank, sf.version, sf.label, sf.name, sf.checksum, "", tStart.UTC(), dt,
	)

	if err != nil {
		return err
	}

	return nil
}
