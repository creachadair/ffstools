package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Filter is a collection of path filters.
type Filter struct {
	Base string
	Rule []*regexp.Regexp

	up *Filter
}

// Load loads a list of path filters from a file.
//
// Blank lines and lines beginning with "#" are ignored.  Otherwise, each line
// gives a path glob that the filter should match.
func (f *Filter) Load(name string) (*Filter, error) {
	path, base := name, ""
	if f != nil {
		base = f.Base
		var err error
		path, err = filepath.Rel(base, name)
		if err != nil {
			return f, nil
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return f, nil
	} else if err != nil {
		return f, err
	}
	subBase := filepath.Dir(filepath.Join(base, path))
	sub, err := parseFilter(subBase, data)
	if err != nil {
		return f, err
	}
	sub.up = f
	return sub, nil
}

// Match reports whether
func (f *Filter) Match(path string) bool {
	if f == nil {
		return false
	}
	adj, _ := filepath.Rel(f.Base, path)
	for _, elt := range f.Rule {
		if elt.MatchString(adj) {
			return true
		}
	}
	return f.up.Match(path)
}

func parseFilter(base string, data []byte) (*Filter, error) {
	f := &Filter{Base: base}
	for i, line := range bytes.Split(data, []byte("\n")) {
		trim := bytes.TrimSpace(line)
		if len(trim) == 0 || trim[0] == '#' {
			continue
		}
		re, err := regexp.Compile(makePattern(string(trim)))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid pattern: %w", i+1, err)
		}
		f.Rule = append(f.Rule, re)
	}
	return f, nil
}

func makePattern(raw string) string {
	r := strings.NewReplacer(
		`\\`, `\\`,
		`\**`, `\\**`, `**`, `.*`,
		`\*`, `\*`, `*`, `[^/]*`,
		`\?`, `\?`, `?`, `[^/]`,
	)
	return `^` + r.Replace(strings.TrimSuffix(raw, "/")) + `$`
}
