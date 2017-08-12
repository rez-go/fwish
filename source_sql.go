package fwish

import (
	"bufio"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
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
	files      []SourceMigration
}

func NewSQLSource(sourceURL string) (Source, error) {
	src := sqlSource{url: sourceURL}
	if err := src.loadMeta(); err != nil {
		return nil, err
	}
	return &src, nil
}

func (s *sqlSource) loadMeta() error {
	//TODO: might want to stream the content
	fd, err := ioutil.ReadFile(
		filepath.Join(s.url, "fwish.yaml"),
	)
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			return errors.Wrap(err, "fwish: error opening source meta file")
		}
		// Try other name
		fd, err = ioutil.ReadFile(
			filepath.Join(s.url, "schema.yaml"),
		)
		if err != nil {
			return errors.Wrap(err, "fwish: unable to open any source meta file")
		}
	}

	//TODO: cache
	meta := sqlSourceMeta{}
	if err := yaml.Unmarshal(fd, &meta); err != nil {
		return errors.Wrap(err, "fwish: unable to load source meta file")
	}

	s.schemaID = meta.ID
	s.schemaName = meta.Name

	return nil
}

func (s *sqlSource) SchemaID() string {
	return s.schemaID
}

func (s *sqlSource) SchemaName() string {
	return s.schemaName
}

func (s *sqlSource) Migrations() ([]SourceMigration, error) {
	if !s.scanned {
		_, err := s.scanSourceDir()
		if err != nil {
			return nil, err
		}
	}
	return s.files, nil
}

func (s *sqlSource) ExecuteMigration(db DB, sm SourceMigration) error {
	//TODO: ensure that the it's our migration
	//TODO: load all the content, checksum, then execute
	fh, err := os.Open(filepath.Join(s.url, sm.Name))
	if err != nil {
		panic(err)
	}
	defer fh.Close()

	var script string
	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	ck := crc32.NewIEEE()
	for scanner.Scan() {
		ck.Write(scanner.Bytes())
		script += scanner.Text() + "\n"
	}

	if sm.Checksum != ck.Sum32() {
		//TODO: more informative message
		return errors.Errorf("fwish: bad source checksum %q", sm.Name)
	}

	_, err = db.Exec(script)
	if err != nil {
		return err
	}

	return nil
}

// returns the number of files?
func (s *sqlSource) scanSourceDir() (numFiles int, err error) {
	// We might want to make these configurable
	sfx := s.fileSuffix
	if sfx == "" {
		sfx = ".sql"
	}
	ignorePrefix := "_"
	versionSep := "__"

	numFiles = 0
	s.files = nil

	fl, err := ioutil.ReadDir(s.url)
	if err != nil {
		return 0, err //TODO: handle no such file (and wrap the error)
	}

	for _, entry := range fl {
		fname := entry.Name()

		if strings.HasPrefix(fname, ignorePrefix) {
			continue
		}
		if !strings.HasSuffix(fname, sfx) {
			continue
		}

		if idx := strings.Index(fname, versionSep); idx > 0 {
			// vstr := fname[:idx]
			// //TODO: replace the underscore with space and do other stuff
			// label := fname[idx+len(versionSep) : len(fname)-len(sfx)]

			//TODO: get the first line of comment as the desc
			cksum, err := s.checksumSourceFile(filepath.Join(s.url, fname))
			if err != nil {
				panic(err)
			}
			if cksum == 0 {
				//TODO: check the reference behavior
				continue
			}

			//TODO: inspect the file?
			//TODO: ensure no files with the same version
			// if _, ok := s.files[vstr]; ok {
			// 	//TODO: write test case for this
			// 	return 0, errors.Errorf("fwish: version %q duplicated", vstr)
			// }

			s.files = append(s.files, SourceMigration{
				Name:     fname,
				Checksum: cksum,
			})
		}
	}

	s.scanned = true

	return len(s.files), nil
}

func (s *sqlSource) checksumSourceFile(filename string) (uint32, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return 0, errors.Wrap(err, "fwish: unable opening file for checksum")
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	ck := crc32.NewIEEE()
	for scanner.Scan() {
		ck.Write(scanner.Bytes())
	}

	return ck.Sum32(), nil
}
