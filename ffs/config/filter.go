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

// Filter is a collection of path filters used to control what parts of a file
// tree get copied by "put" operations. Each line of a filter file specifies a
// glob matching a filename. By default, all files are included, and any file
// matching a rule is filtered out.
//
// Each directory is checked for a filter file, which governs paths under that
// directory. If a rule is prefixed with "!", a match of that rule inverts the
// sense of the parent directory for that path, including it if it was filtered
// and vice versa.
type Filter struct {
	Base string
	Rule []Rule

	up *Filter
}

// A Rule is a single filter rule.
type Rule struct {
	*regexp.Regexp      // the compiled glob expression
	Negate         bool // whether this rule negates a parent match
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

	data, err := os.ReadFile(name)
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

// Match reports whether path should be excluded.
func (f *Filter) Match(path string) bool {
	if f == nil {
		return false
	}
	adj, _ := filepath.Rel(f.Base, path)
	for _, elt := range f.Rule {
		if elt.MatchString(adj) {
			// If this is a negative match, it reverses the previous judgement.
			if elt.Negate {
				return !f.up.Match(path)
			}
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
		negate, pat := trimPrefix(string(trim), "!")
		re, err := regexp.Compile(makePattern(pat))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid pattern: %w", i+1, err)
		}
		f.Rule = append(f.Rule, Rule{
			Regexp: re,
			Negate: negate,
		})
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

func trimPrefix(s, pfx string) (bool, string) {
	if strings.HasPrefix(s, pfx) {
		return true, strings.TrimPrefix(s, pfx)
	}
	return false, s
}
