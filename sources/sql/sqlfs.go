package sql

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io/fs"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rez-go/fwish"
)

type sqlFSSource struct {
	schemaID   string
	schemaName string
	fs         fs.FS
	fileSuffix string
	scanned    bool
	migrations []fwish.MigrationInfo
}

func LoadFS(fs fs.FS) (fwish.MigrationSource, error) {
	fh, err := fs.Open("fwish.yaml")
	if err != nil {
		return nil, fmt.Errorf("fwish.sql: unable to open schema index file: %w", err)
	}
	defer fh.Close()

	idx := sqlSourceMeta{}
	ydec := yaml.NewDecoder(fh)
	if err := ydec.Decode(&idx); err != nil {
		return nil, fmt.Errorf("fwish.sql: unable to load schema index file: %w", err)
	}

	return &sqlFSSource{
		schemaID:   idx.ID,
		schemaName: idx.Name,
		fs:         fs,
	}, nil
}

func (src *sqlFSSource) SchemaID() string {
	return src.schemaID
}

func (src *sqlFSSource) SchemaName() string {
	return src.schemaName
}

func (src *sqlFSSource) Migrations() ([]fwish.MigrationInfo, error) {
	if !src.scanned {
		_, err := src.scanSourceDir()
		if err != nil {
			return nil, err
		}
	}
	return src.migrations, nil
}

func (src *sqlFSSource) ExecuteMigration(db fwish.DB, sm fwish.MigrationInfo) error {
	//TODO: ensure that the it's our migration
	//TODO: load all the content, checksum, then execute
	fh, err := src.fs.Open(sm.Script)
	if err != nil {
		return fmt.Errorf("fwish.sql: unable to load migration file: %w", err)
	}
	defer fh.Close()

	var script string
	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	ck := crc32.NewIEEE()
	for scanner.Scan() {
		_, err = ck.Write(scanner.Bytes())
		if err != nil {
			return err
		}
		script += scanner.Text() + "\n"
	}

	if sm.Checksum != ck.Sum32() {
		return fmt.Errorf("fwish.sql: bad migration file checksum %q", sm.Name)
	}

	_, err = db.Exec(script)
	return err
}

// returns the number of files?
func (src *sqlFSSource) scanSourceDir() (numFiles int, err error) {
	sfx := src.fileSuffix
	if sfx == "" {
		sfx = ".sql"
	}
	ignorePrefix := "_"

	src.migrations = nil

	fl, err := fs.ReadDir(src.fs, ".")
	if err != nil {
		return 0, fmt.Errorf("fwish.sql: unable to read migration directory: %w", err)
	}

	for _, entry := range fl {
		fname := entry.Name()

		if strings.HasPrefix(fname, ignorePrefix) {
			continue
		}
		if !strings.HasSuffix(fname, sfx) {
			continue
		}

		//TODO: we can optimize this by using goroutines
		cksum, err := src.checksumSourceFile(fname)
		if err != nil {
			return 0, err
		}
		if cksum == 0 {
			// Empty file
			//TODO: check the reference behavior
			continue
		}

		src.migrations = append(src.migrations, fwish.MigrationInfo{
			Name:     fname[:len(fname)-len(sfx)],
			Script:   fname,
			Checksum: cksum,
		})
	}

	src.scanned = true

	return len(src.migrations), nil
}

func (src *sqlFSSource) checksumSourceFile(filename string) (uint32, error) {
	fh, err := src.fs.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("fwish.sql: unable to load migration file: %w", err)
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	ck := crc32.NewIEEE()
	for scanner.Scan() {
		_, err = ck.Write(scanner.Bytes())
		if err != nil {
			return 0, err
		}
	}

	return ck.Sum32(), nil
}
