package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/rez-go/fwish"
	sqlsource "github.com/rez-go/fwish/sources/sql"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Execute migration",
	Run: func(cmd *cobra.Command, args []string) {
		if migrateSource == "" {
			fmt.Fprintf(os.Stderr, "Source directory is required\n")
			return
		}
		if migrateDBURL == "" {
			fmt.Fprintf(os.Stderr, "Database connection string is required\n")
			return
		}

		parsedURL, err := url.Parse(migrateDBURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to parse database connection string\n")
			return
		}
		username := parsedURL.User.Username()

		logger := log.New(os.Stderr, "", log.LstdFlags)

		mg, err := fwish.NewMigrator("")
		if err != nil {
			panic(err)
		}
		mg.WithLogger(logger)
		mg.WithUserID(username)
		src, err := sqlsource.LoadDir(migrateSource)
		if err != nil {
			if err == fwish.ErrSchemaIndexFileNotFound {
				logger.Fatal("Source does not contain fwish.yaml file")
			}
			panic(err)
		}
		err = mg.AddSource(src)
		if err != nil {
			panic(err)
		}

		db, err := sql.Open("postgres", migrateDBURL)
		if err != nil {
			logger.Fatal(err)
		}

		t0 := time.Now()

		n, err := mg.Migrate(db, "")
		if err != nil {
			logger.Fatal(err)
		}

		schemaName := src.SchemaName()

		if n == 0 {
			logger.Printf("Schema %q is up to date.",
				schemaName)
		} else {
			logger.Printf("Successfully applied %d migrations to schema %q (execution time %s)",
				n, schemaName, time.Since(t0).String())
		}
	},
}

var (
	migrateSource string
	migrateDBURL  string
)

func init() {
	migrateCmd.Flags().StringVarP(&migrateSource, "source", "s", "", "Source directory to read from")
	migrateCmd.Flags().StringVarP(&migrateDBURL, "db", "", "", "Database connection string e.g., postgres://username:password@localhost:5432/tablename?sslmode=disable")

	rootCmd.AddCommand(migrateCmd)
}
