// Copyright 2021 Michael J. Fromberger. All Rights Reserved.
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

package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/creachadair/ffstools/ffs/config"
)

func TestPolicyRule(t *testing.T) {
	type rule struct {
		path, allow, deny string
	}
	r2p := func(rs []rule) (out config.Policy) {
		for _, r := range rs {
			out = append(out, config.PolicyRule{
				Path:  strings.Fields(r.path),
				Allow: strings.Fields(r.allow),
				Deny:  strings.Fields(r.deny),
			})
		}
		return
	}
	sub := config.Policy.CheckSub
	kv := config.Policy.CheckKV
	tests := []struct {
		label      string
		rules      []rule
		path, name string
		call       func(config.Policy, context.Context, []string, string) error
		wantOK     bool
	}{
		{"Empty/Sub", nil, "a b c", "d", sub, true},
		{"Empty/KV", nil, "a b c", "d", kv, true},
		{"RootOnly/Sub", []rule{{"", "*", ""}}, "", "x", sub, false},
		{"RootOnly/KV", []rule{{"", "*", ""}}, "", "x", kv, true},
		{"Root1/Sub2", []rule{{"?", "*", ""}}, "w", "x", sub, false},
		{"Root1/Sub1", []rule{{"?", "*", ""}}, "", "2", sub, true},
		{"Root1/KV", []rule{{"?", "*", ""}}, "w", "x", kv, true},
		{"Root1/KVdeny", []rule{{"?", "*", "q"}}, "w", "q", kv, false},
		{"Root1/KVdeny", []rule{{"?", "*", "q"}}, "w", "r", kv, true},
		{"Root/Deep0", []rule{{"a * b", "*", ""}}, "a", "b", sub, true},
		{"Root/Deep1", []rule{{"a * b", "*", ""}}, "a c", "b", sub, true},
		{"Root/Deep2", []rule{{"a * b", "*", ""}}, "a c d", "b", sub, true},
		{"Root/Q0", []rule{{"a ?", "*", ""}}, "", "a", sub, false},
		{"Root/Q1", []rule{{"a ?", "*", ""}}, "a", "x", sub, true},
		{"Root/Qkv", []rule{{"a ?", "*", ""}}, "a x", "b", kv, true},
		{"Override/Top/Y", []rule{
			{"*", "xyzzy", ""}, {"a b *", "*", "xyzzy"},
		}, "a", "xyzzy", kv, true},
		{"Override/Top/N", []rule{
			{"*", "xyzzy", ""}, {"a b *", "*", "xyzzy"},
		}, "a", "bad", kv, false},
		{"Override/Inner/Y", []rule{
			{"*", "xyzzy", ""}, {"a b *", "*", "xyzzy"},
		}, "a b", "other", kv, true},
		{"Override/Inner/N", []rule{
			{"*", "xyzzy", ""}, {"a b *", "*", "xyzzy"},
		}, "a b", "xyzzy", kv, false},
	}
	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			p := r2p(tc.rules)
			input := strings.Fields(tc.path)
			got := tc.call(p, t.Context(), input, tc.name)
			if (got == nil) != tc.wantOK {
				t.Errorf("Check %q %q: err=%v, want %v", input, tc.name, got, tc.wantOK)
			}
		})
	}
}
