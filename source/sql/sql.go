package sql

import (
	"bufio"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"bitbucket.org/exavolt/fwish"
)

//TODO: dialect / engine etc.
type sqlSourceMeta struct {
	ID         string
	Name       string
	FileSuffix string
}

type sqlSource struct {
	schemaID   string
	schemaName string
	url        string
	fileSuffix string
	scanned    bool
	files      []fwish.MigrationInfo
}

// NewMigrator is a helper function to create a migrator if there's
// only one source.
func NewMigrator(schemaID string, sourceURL string) (*fwish.Migrator, error) {
	mg, err := fwish.NewMigrator(schemaID)
	if err != nil {
		return nil, err
	}
	src, err := Load(sourceURL)
	if err != nil {
		return nil, err
	}
	err = mg.AddSource(src)
	if err != nil {
		return nil, err
	}
	return mg, nil
}

// Load creates a SQL-based migration source from specified URL.
func Load(sourceURL string) (fwish.MigrationSource, error) {
	src := sqlSource{url: sourceURL}

	//TODO: stream the content
	fd, err := ioutil.ReadFile(
		filepath.Join(src.url, "fwish.yaml"),
	)
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			return nil, errors.Wrap(err, "fwish.sql: error opening source meta file")
		}
		// Try other name
		fd, err = ioutil.ReadFile(
			filepath.Join(src.url, "schema.yaml"),
		)
		if err != nil {
			return nil, errors.Wrap(err, "fwish.sql: unable to open any source meta file")
		}
	}

	meta := sqlSourceMeta{}
	if err := yaml.Unmarshal(fd, &meta); err != nil {
		return nil, errors.Wrap(err, "fwish.sql: unable to load meta file")
	}

	src.schemaID = meta.ID
	src.schemaName = meta.Name

	return &src, nil
}

func (src *sqlSource) SchemaID() string {
	return src.schemaID
}

func (src *sqlSource) SchemaName() string {
	return src.schemaName
}

func (src *sqlSource) Migrations() ([]fwish.MigrationInfo, error) {
	if !src.scanned {
		_, err := src.scanSourceDir()
		if err != nil {
			return nil, err
		}
	}
	return src.files, nil
}

func (src *sqlSource) ExecuteMigration(db fwish.DB, sm fwish.MigrationInfo) error {
	//TODO: ensure that the it's our migration
	//TODO: load all the content, checksum, then execute
	fh, err := os.Open(filepath.Join(src.url, sm.Script))
	if err != nil {
		return errors.Wrap(err, "fwish.sql: unable to load migration file")
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
		//TODO: more informative message
		return errors.Errorf("fwish.sql: bad migration file checksum %q", sm.Name)
	}

	_, err = db.Exec(script)
	return err
}

// returns the number of files?
func (src *sqlSource) scanSourceDir() (numFiles int, err error) {
	sfx := src.fileSuffix
	if sfx == "" {
		sfx = ".sql"
	}
	ignorePrefix := "_"

	src.files = nil

	fl, err := ioutil.ReadDir(src.url)
	if err != nil {
		return 0, errors.Wrap(err, "fwish.sql: unable to read migration directory")
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
		cksum, err := src.checksumSourceFile(filepath.Join(src.url, fname))
		if err != nil {
			return 0, err
		}
		if cksum == 0 {
			// Empty file
			//TODO: check the reference behavior
			continue
		}

		src.files = append(src.files, fwish.MigrationInfo{
			Name:     fname[:len(fname)-len(sfx)],
			Script:   fname,
			Checksum: cksum,
		})
	}

	src.scanned = true

	return len(src.files), nil
}

func (src *sqlSource) checksumSourceFile(filename string) (uint32, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return 0, errors.Wrap(err, "fwish.sql: unable to load migration file")
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
