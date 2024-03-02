package fwish_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/rez-go/fwish"
	sqlsource "github.com/rez-go/fwish/sources/sql"
)

type sourceConflict struct {
}

func (s *sourceConflict) SchemaID() string   { return "" }
func (s *sourceConflict) SchemaName() string { return "" }

func (s *sourceConflict) Migrations() ([]fwish.MigrationInfo, error) {
	return []fwish.MigrationInfo{
		{Name: "V1__test"},
		{Name: "V1__test2"},
	}, nil
}

func (s *sourceConflict) ExecuteMigration(db fwish.DB, migration fwish.MigrationInfo) error {
	return errors.New("not implemented")
}

func TestVersionConflict(t *testing.T) {
	mg, err := fwish.NewMigrator("")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(&sourceConflict{})
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), `version "1" conflict`) {
		t.Fatal("wrong error message")
	}
}

func TestSchemaIDMatchSourceProgram(t *testing.T) {
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
}

func TestSchemaIDMismatchSourceProgram(t *testing.T) {
	mg, err := fwish.NewMigrator("myapp.example.com")
	if err != nil {
		t.Fatal(err)
	}
	src, err := sqlsource.Load("./test-data/basic")
	if err != nil {
		t.Fatal(err)
	}
	err = mg.AddSource(src)
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), "schema ID mismatch") {
		t.Error(err)
	}
}
