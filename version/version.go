package version

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Version holds info of parsed version
type Version []int64

// String returns a normalized string
func (v Version) String() string {
	if len(v) == 0 {
		return ""
	}
	sl := make([]string, len(v))
	for i, iv := range v {
		sl[i] = strconv.FormatInt(iv, 10)
	}
	return strings.Join(sl, ".")
}

// Parse parses a string and returns a normalized string and a list of
// int64.
func Parse(vstr string) (Version, error) {
	if vstr == "" {
		return nil, nil
	}
	pl := strings.Split(vstr, ".")
	if len(pl) == 1 {
		// Try underscore
		// We support underscore for compatibility.
		// some sources might using class name for the migration name.
		pl = strings.Split(vstr, "_")
	}
	ints := make([]int64, len(pl))
	for i, sv := range pl {
		// note that we don't need to trim left zeroes as we explicitly
		// tell the parser that the number is a decimal.
		iv, err := strconv.ParseInt(sv, 10, 64)
		if err != nil {
			if strings.HasSuffix(err.Error(), " invalid syntax") {
				return nil, fmt.Errorf("fwish.version: invalid version syntax")
			}
			return nil, err
		}
		ints[i] = iv
	}
	return Version(ints), nil
}

// SortStrings sort a list of version string.
func SortStrings(versions []string) error {
	if len(versions) == 0 {
		return nil
	}
	type item struct {
		v    Version
		orig string
	}
	items := make([]item, len(versions))
	for i, s := range versions {
		v, err := Parse(s)
		if err != nil {
			return err
		}
		items[i] = item{v, s}
	}
	sort.SliceStable(items, func(i, j int) bool {
		vlA := items[i].v
		vlB := items[j].v
		var mx int
		if len(vlA) < len(vlB) {
			mx = len(vlA)
		} else {
			mx = len(vlB)
		}
		for k := 0; k < mx; k++ {
			if vlA[k] < vlB[k] {
				return true
			}
			if vlA[k] > vlB[k] {
				return false
			}
		}
		return len(vlA) < len(vlB)
	})
	for i, it := range items {
		versions[i] = it.orig
	}
	return nil
}
