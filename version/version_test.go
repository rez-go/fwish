package version_test

import (
	"strings"
	"testing"

	"github.com/exavolt/fwish/version"
)

func TestParse(t *testing.T) {
	cases := []struct {
		input      string
		normalized string
		ints       []int64
		err        error
		errMsg     string
	}{
		{"", "", nil, nil, ""},
		{"1", "1", []int64{1}, nil, ""},
		{"0", "0", []int64{0}, nil, ""},
		{"001", "1", []int64{1}, nil, ""},
		{"000", "0", []int64{0}, nil, ""},
		{"1.0", "1.0", []int64{1, 0}, nil, ""},
		{"01.00", "1.0", []int64{1, 0}, nil, ""},
		{"a", "", nil, nil, "invalid version syntax"},
	}

	for i, c := range cases {
		il, err := version.Parse(c.input)
		if err != nil {
			if c.err != nil {
				if c.err != err {
					t.Errorf("#%d: expected %v, got %v", i+1, c.err, err)
				}
			} else if c.errMsg != "" {
				if !strings.Contains(err.Error(), c.errMsg) {
					t.Errorf("#%d: expected %s, got %s", i+1, c.errMsg, err.Error())
				}
			} else {
				t.Errorf("#%d: expected no errors, got %v", i+1, err)
			}
		} else if c.err != nil {
			t.Errorf("#%d: expected %v, got no errors", i+1, c.err)
		} else if c.errMsg != "" {
			t.Errorf("#%d: expected %s, got no errors", i+1, c.errMsg)
		}
		if il.String() != c.normalized {
			t.Errorf("#%d: expected %q, got %q", i+1, c.normalized, il.String())
		}
		if !intsEq(c.ints, il) {
			t.Errorf("#%d: ints are inequal", i+1)
		}
	}
}

func TestSort(t *testing.T) {
	cases := []struct {
		input  []string
		output []string
		err    error
	}{
		{[]string{""}, []string{""}, nil},
		{[]string{"1"}, []string{"1"}, nil},
		{[]string{"2", "1"}, []string{"1", "2"}, nil},
		{[]string{"1.1", "1", "1.2", "1.0"}, []string{"1", "1.0", "1.1", "1.2"}, nil},
		{
			[]string{"002.0002", "10", "1", "2", "3.10", "3.1", "3", "4_2"},
			[]string{"1", "2", "002.0002", "3", "3.1", "3.10", "4_2", "10"},
			nil,
		},
	}

	for i, c := range cases {
		err := version.SortStrings(c.input)
		if err != nil {
			if c.err == nil {
				t.Errorf("#%d: expected %v, got %v", i+1, c.err, err)
			} else if err.Error() != c.err.Error() {
				t.Errorf("#%d: expected %q, got %q", i+1, c.err.Error(), err.Error())
			}
		} else if c.err != nil {
			t.Errorf("#%d: expected %v, got %v", i+1, c.err, err)
		}
		if !stringsEq(c.input, c.output) {
			t.Errorf("#%d:\n\texpected: %v\n\tgot: %v", i+1, c.output, c.input)
		}
	}
}

func intsEq(a, b []int64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func stringsEq(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
