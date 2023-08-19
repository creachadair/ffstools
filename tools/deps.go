//go:build go_mod_tidy_deps

// Package tools is a pseudo-package of Go tool dependencies not required for
// building or testing.
package tools

import (
	_ "honnef.co/go/tools/cmd/staticcheck"
)
