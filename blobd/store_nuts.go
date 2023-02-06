//go:build all || nuts

package main

import "github.com/creachadair/nutstore"

func init() { stores["nuts"] = nutstore.Opener }
