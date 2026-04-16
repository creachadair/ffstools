// Copyright 2022 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package putlib

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
// glob matching file paths. By default, all files are included, and any path
// matching a rule is filtered out.
//
// Each directory may specify its own filter, which governs paths under that
// directory. If a rule is prefixed with "!", a match of that rule overrides an
// exclusion, either from the directory itself or a parent filters.
type Filter struct {
	Base string
	Rule []Rule

	up *Filter
}

// A Rule is a single filter rule.
type Rule struct {
	*regexp.Regexp      // the compiled glob expression
	Negate         bool // if true, override a previous exclusion
}

// LoadFile loads a list of path filters from a file.
//
// Blank lines and lines beginning with "#" are ignored.  Otherwise, each line
// gives a path glob that the filter should match.
func (f *Filter) LoadFile(name string) (*Filter, error) {
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

// Exclude reports whether path should be excluded.
func (f *Filter) Exclude(path string) bool {
	if f == nil {
		return false
	}
	adj, _ := filepath.Rel(f.Base, path)

	// Within a rule set, exclusions may be overruled by negations.
	// For example, given rules:
	//
	//     foo/*
	//     !foo/bar
	//
	// the first rule excludes everything in foo/, but the second rule excepts
	// foo/bar, so we have to check all the rules before making a decision.
	// A path is excluded if it matches any non-negated rules and no negated
	// rules.
	exclude := f.up.Exclude(path)
	for _, elt := range f.Rule {
		if elt.MatchString(adj) {
			if elt.Negate {
				// A negated rule match overrules any previous exclusion judgement,
				// either from the parent (above) or another rule in this filter.
				return false
			} else {
				exclude = true
			}
		}
	}
	return exclude
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
	if after, ok := strings.CutPrefix(s, pfx); ok {
		return true, after
	}
	return false, s
}
