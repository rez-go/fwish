package fwish_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/exavolt/fwish"
	sqlsource "github.com/exavolt/fwish/source/sql"
)

//TODO:
// - we might want to have the migration file contents here in the code.
//   if we need the files, we make a temp dir and write the files there.
//   this way, we can be sure that the source files are always consistent.
// - test with search_path set to other schema
// - test with SQLs which use schema explicitly (other than defined schema)

const dsn = "postgres:///?sslmode=disable"

// The schema name we use in the tests. This schema will be dropped
// and recreated multiple times.
const testSchemaName = "__fwishtest"

// Very basic functional test
func TestBasic(t *testing.T) {
	mg, err := fwish.NewMigrator("372ce18d-02a2-4cb1-828a-bb470f02fe6e")
	if err != nil {
		t.Fatal(err)
	}
	src, err := sqlsource.Load("./test-data/basic")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(src)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + testSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}

	//TODO: check the number of applied migrations
	_, err = mg.Migrate(db, testSchemaName)
	if err != nil {
		t.Fatal(err)
	}
	//TODO: validate

	_, err = db.Exec(`DROP SCHEMA ` + testSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
}

// Using sqlx instead of stdlib's sql package.
func TestBasicSQLX(t *testing.T) {
	mg, err := fwish.NewMigrator("372ce18d-02a2-4cb1-828a-bb470f02fe6e")
	if err != nil {
		t.Fatal(err)
	}
	src, err := sqlsource.Load("./test-data/basic")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(src)
	if err != nil {
		t.Fatal(err)
	}

	// The only difference
	db, err := sqlx.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + testSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}

	//TODO: check the number of applied migrations
	_, err = mg.Migrate(db, testSchemaName)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`DROP SCHEMA ` + testSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchemaID(t *testing.T) {
	// Match: source-program
	mg, err := fwish.NewMigrator("372ce18d-02a2-4cb1-828a-bb470f02fe6e")
	if err != nil {
		t.Fatal(err)
	}
	src, err := sqlsource.Load("./test-data/basic")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(src)
	if err != nil {
		t.Fatal(err)
	}

	// Mismatch: source-program
	mg, err = fwish.NewMigrator("myapp.example.com")
	if err != nil {
		t.Fatal(err)
	}
	src, err = sqlsource.Load("./test-data/basic")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(src)
	if err == nil {
		t.Error("error expected")
	} else if !strings.Contains(err.Error(), "schema ID mismatch") {
		t.Error(err)
	}

	//TODO: test the one in the DB
}

func TestBadRank(t *testing.T) {
	//TODO:
	// apply migrations, alter or delete a row from meta table, then validate
}
