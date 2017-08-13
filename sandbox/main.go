package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/exavolt/fwish"
	sqlsource "github.com/exavolt/fwish/source/sql"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	//TODO:
	// mg := fwish.NewMigrator(schemaID)
	// src, err := fwish.source.sql.Load(url)
	// err := mg.AddSource(src)
	// gs, err := fwish.source.go.LoadFromURL(url)
	// err := mg.AddSource(src)
	// gs, err := fwish.source.go.Load(app.MigrationSource())
	// x := fwish.NewContext(db, &fwish.Options{schemaName, metaTableName, ...})
	// n, err = mg.Migrate(x)
	// x.Err() // sticky error
	// x.InstalledRank()
	//
	//TODO: sources:
	// - sql files inside a dir from URL. could be local, could be remote.
	//   for remote, we might want to pull all of them.
	// - go interface implementation. simple.
	// - go package. might not that practical as we'll probably need to
	//   compile the source. could be generalized / abstracted as 'executable'.
	// - executable source. we should be able to get meta out of it.
	//   and we'll just execute the executable to perform the migration.

	mg, err := fwish.NewMigrator("372ce18d-02a2-4cb1-828a-bb470f02fe6e")
	if err != nil {
		panic(err)
	}
	mg.WithLogger(logger)
	mg.WithUserID("sandbox")
	src, err := sqlsource.Load("../test-data/basic")
	if err != nil {
		panic(err)
	}
	err = mg.AddSource(src)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", "postgres:///?sslmode=disable")
	if err != nil {
		logger.Fatal(err)
	}

	schemaName := "__fwishsandbox"

	t0 := time.Now()
	// t, err :=fwish.NewTarget(db, options)
	// mg.Migrate(t)
	n, err := mg.Migrate(db, schemaName)
	if err != nil {
		logger.Fatal(err)
	}

	if n == 0 {
		logger.Printf("Schema %q is up to date. No migrations necessary.",
			schemaName)
	} else {
		logger.Printf("Successfully applied %d migrations to schema %q (execution time %s)",
			n, schemaName, time.Since(t0).String())
	}
}
