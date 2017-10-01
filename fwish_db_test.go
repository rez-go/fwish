package fwish_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"

	"bitbucket.org/exavolt/fwish"
	sqlsource "bitbucket.org/exavolt/fwish/source/sql"
)

//TODO:
// - we might want to have the migration file contents here in the code.
//   if we need the files, we make a temp dir and write the files there.
//   this way, we can be sure that the source files are always consistent.
// - test with search_path set to other schema
// - test with SQLs which use schema explicitly (other than defined schema)
// - test multiple sources

var testDBDSN = "postgres:///?sslmode=disable"

// The schema name we use in the tests. This schema will be dropped
// and recreated multiple times.
var testDBSchemaName = "__fwishtest"

func init() {
	if s, exists := os.LookupEnv("TEST_DB_DSN"); exists {
		testDBDSN = s
	}
	if s, exists := os.LookupEnv("TEST_DB_SCHEMA_NAME"); exists {
		testDBSchemaName = s
	}
}

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

	db, err := sql.Open("postgres", testDBDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + testDBSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}

	//TODO: check the number of applied migrations
	_, err = mg.Migrate(db, testDBSchemaName)
	if err != nil {
		t.Fatal(err)
	}
	//TODO: validate
	// Apply the migration again
	n, err := mg.Migrate(db, testDBSchemaName)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("0 expected, got %d", n)
	}

	//TODO: should be a defered statement
	_, err = db.Exec(`DROP SCHEMA ` + testDBSchemaName + ` CASCADE`)
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
	db, err := sqlx.Open("postgres", testDBDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DROP SCHEMA IF EXISTS ` + testDBSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}

	//TODO: check the number of applied migrations
	_, err = mg.Migrate(db, testDBSchemaName)
	if err != nil {
		t.Fatal(err)
	}
	//TODO: validate
	// Apply the migration again
	n, err := mg.Migrate(db, testDBSchemaName)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("0 expected, got %d", n)
	}

	//TODO: should be a defered statement
	_, err = db.Exec(`DROP SCHEMA ` + testDBSchemaName + ` CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSchemaID(t *testing.T) {
	//TODO: test the one in the DB
}

func TestBadRank(t *testing.T) {
	//TODO:
	// apply migrations, alter or delete a row from meta table, then validate
}
