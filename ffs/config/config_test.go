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
		path, allow string
	}
	r2p := func(rs ...rule) (out config.Policy) {
		for _, r := range rs {
			out = append(out, config.PolicyRule{
				Path:  strings.Fields(r.path),
				Allow: strings.Fields(r.allow),
			})
		}
		return
	}
	sub := config.Policy.CheckSub
	kv := config.Policy.CheckKV
	tests := []struct {
		label      string
		policy     config.Policy
		path, name string
		call       func(config.Policy, context.Context, []string, string) error
		wantOK     bool
	}{
		{"empty/1sub", nil, "", "x", sub, true},
		{"empty/0kv", nil, "", "x", kv, true},
		{"empty/Nsub", nil, "a b c", "d", sub, true},
		{"empty/Nkv", nil, "a b c", "x", kv, true},

		{"root_only/sub", r2p(rule{path: ""}), "", "a", sub, false},
		{"root_only/kv", r2p(rule{path: ""}), "", "x", kv, true},

		{"root_only_allow/sub", r2p(rule{"", "x y"}), "", "a", sub, false},
		{"root_only_allow/0kv/y", r2p(rule{"", "x y"}), "", "x", kv, true},
		{"root_only_allow/0kv/n", r2p(rule{"", "x y"}), "", "z", kv, false},

		{"no_root/1sub/y", r2p(rule{path: "a"}, rule{path: "b"}), "", "a", sub, true},
		{"no_root/1sub/n", r2p(rule{path: "a"}, rule{path: "b"}), "", "c", sub, false},
		{"no_root/0kv/n", r2p(rule{path: "a"}, rule{path: "b"}), "", "x", kv, false},
		{"no_root/1kv/y", r2p(rule{path: "a"}, rule{path: "b"}), "a", "x", kv, true},
		{"no_root/1kv/n", r2p(rule{path: "a"}, rule{path: "b"}), "c", "x", kv, false},

		{"no_root_allow/1kv/ya", r2p(
			rule{path: "a", allow: "x y"}, rule{path: "b", allow: "z"},
		), "a", "x", kv, true},
		{"no_root_allow/1kv/na", r2p(
			rule{path: "a", allow: "x y"}, rule{path: "b", allow: "z"},
		), "a", "z", kv, false},
		{"no_root_allow/1kv/yb", r2p(
			rule{path: "a", allow: "x y"}, rule{path: "b", allow: "z"},
		), "b", "z", kv, true},
		{"no_root_allow/1kv/nb", r2p(
			rule{path: "a", allow: "x y"}, rule{path: "b", allow: "z"},
		), "b", "x", kv, false},

		{"no_root_allow/all_x/1kv/ya", r2p(
			rule{path: "? *", allow: "x"}, rule{path: "a", allow: "y z"},
		), "a", "x", kv, true},
		{"no_root_allow/all_x/1kv/yc", r2p(
			rule{path: "? *", allow: "x"}, rule{path: "a", allow: "y z"},
		), "c", "x", kv, true},
		{"no_root_allow/all_x/1kv/nb", r2p(
			rule{path: "? *", allow: "x"}, rule{path: "a", allow: "y z"},
		), "b", "z", kv, false},
		{"no_root_allow/all_x/1kv/ya", r2p(
			rule{path: "? *", allow: "x"}, rule{path: "a", allow: "y z"},
		), "a", "z", kv, true},

		{"open_bucket/0kv/y", r2p(
			rule{path: "", allow: "x y"}, rule{path: "open"},
		), "", "x", kv, true},
		{"open_bucket/0kv/n", r2p(
			rule{path: "", allow: "x y"}, rule{path: "open"},
		), "", "z", kv, false},
		{"open_bucket/1kv/yx", r2p(
			rule{path: "", allow: "x y"}, rule{path: "open"},
		), "open", "x", kv, true},
		{"open_bucket/1kv/yz", r2p(
			rule{path: "", allow: "x y"}, rule{path: "open"},
		), "open", "z", kv, true},
	}
	for _, tc := range tests {
		t.Run(tc.label, func(t *testing.T) {
			input := strings.Fields(tc.path)
			got := tc.call(tc.policy, t.Context(), input, tc.name)
			if (got == nil) != tc.wantOK {
				t.Errorf("Check %q %q: err=%v, want %v", input, tc.name, got, tc.wantOK)
			}
		})
	}
}
