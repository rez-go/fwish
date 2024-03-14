package sql

import (
	"os"

	"github.com/rez-go/fwish"
)

// LoadDir creates a SQL-based migration source from specified URL.
func LoadDir(dirPath string) (fwish.MigrationSource, error) {
	fs := os.DirFS(dirPath)
	return LoadFS(fs)
}

// TODO: dialect / engine etc.
type sqlSourceMeta struct {
	ID         string
	Name       string
	FileSuffix string
}
