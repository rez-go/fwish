package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/rez-go/fwish"
	sqlsource "github.com/rez-go/fwish/sources/sql"
)

func main() {
	logger := log.New(os.Stderr, "", log.LstdFlags)

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
