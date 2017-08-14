package version

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

//TODO: probably should create type for the ints
// type Version []int64

// Parse parses a string and returns a normalized string and a list of
// int64.
//
func Parse(vstr string) (normalized string, ints []int64, err error) {
	if vstr == "" {
		return "", nil, nil
	}
	//TODO: we might want to support underscore for compatibility.
	// some source might using class name for the migration name.
	pl := strings.Split(vstr, ".")
	if len(pl) == 1 {
		// Try underscore
		pl = strings.Split(vstr, "_")
	}
	ints = make([]int64, len(pl))
	for i, sv := range pl {
		// note that we don't need to trim left zeroes as we explicitly
		// tell the parser that the number is a decimal.
		iv, err := strconv.ParseInt(sv, 10, 64)
		if err != nil {
			return "", nil, fmt.Errorf("fwish.version: version string %q contains invalid value", vstr)
		}
		ints[i] = iv
	}
	// Convert them back to string
	sl := make([]string, len(ints))
	for i, iv := range ints {
		sl[i] = strconv.FormatInt(iv, 10)
	}
	return strings.Join(sl, "."), ints, nil
}

// SortStrings sort a list of version string.
//
//TODO: should we treat 1.0 as equal to 1?
func SortStrings(versions []string) error {
	if len(versions) == 0 {
		return nil
	}
	type item struct {
		ints []int64
		str  string
	}
	items := make([]item, len(versions))
	for i, s := range versions {
		_, ints, err := Parse(s)
		if err != nil {
			return err
		}
		items[i] = item{ints, s}
	}
	sort.Slice(items, func(i, j int) bool {
		vlA := items[i].ints
		vlB := items[j].ints
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
		versions[i] = it.str
	}
	return nil
}
