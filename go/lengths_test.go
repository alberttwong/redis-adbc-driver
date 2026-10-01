// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"encoding/json"
	"testing"
)

func TestCutRunes(t *testing.T) {
	for _, tc := range []struct {
		s    string
		n    int
		want string
		long bool
	}{
		{"abc", 3, "abc", false},
		{"abcd", 3, "abc", true},
		{"ab", 3, "ab", false},
		{"", 1, "", false},
		{"ééé€x", 4, "ééé€", true},
		{"😀😀😀", 2, "😀😀", true},
		{"a\xffb", 2, "a\xff", true}, // an invalid byte counts as one character
	} {
		got, long := cutRunes(tc.s, tc.n)
		if got != tc.want || long != tc.long {
			t.Errorf("cutRunes(%q, %d) = %q, %v; want %q, %v", tc.s, tc.n, got, long, tc.want, tc.long)
		}
	}
}

func TestFitLength(t *testing.T) {
	vc3 := ColType{Kind: KindString, Length: 3}
	ch3 := ColType{Kind: KindString, Length: 3, Fixed: true}
	for _, tc := range []struct {
		t       ColType
		in      string
		want    string
		wantErr string
	}{
		{vc3, "ab", "ab", ""},
		{vc3, "abc", "abc", ""},
		{vc3, "abc   ", "abc", ""},
		{vc3, "a     ", "a  ", ""},
		{vc3, "abcd", "", "value too long for type character varying(3)"},
		{vc3, "abc d", "", "value too long for type character varying(3)"},
		{vc3, "abc\t", "", "value too long for type character varying(3)"}, // only spaces are cut
		{ch3, "ab ", "ab ", ""},
		{ch3, "abcd", "", "value too long for type character(3)"},
		{typeString, "abcdef", "abcdef", ""},
	} {
		v, err := Coerce(stringValue(tc.in), tc.t)
		if err != nil {
			t.Fatal(err)
		}
		got, err := fitLength(tc.t, v)
		switch {
		case tc.wantErr != "":
			if err == nil || errText(err) != tc.wantErr {
				t.Errorf("fitLength(%s, %q): error %v, want %q", tc.t.SQLName(), tc.in, err, tc.wantErr)
			}
		case err != nil || got.S != tc.want:
			t.Errorf("fitLength(%s, %q) = %q, %v; want %q", tc.t.SQLName(), tc.in, got.S, err, tc.want)
		}
	}
}

// The length and Fixed are optional in the metadata: a type stored before
// they existed reads as unbounded VARCHAR.
func TestColTypeLengthJSON(t *testing.T) {
	for _, ct := range []ColType{typeString, {Kind: KindString, Length: 3}, {Kind: KindString, Length: 4, Fixed: true},
		{Kind: KindString, Fixed: true}, decimalType(10, 2)} {
		b, err := json.Marshal(ct)
		if err != nil {
			t.Fatal(err)
		}
		var back ColType
		if err := json.Unmarshal(b, &back); err != nil || back != ct {
			t.Errorf("%s: round trip %s gives %+v, %v", ct.SQLName(), b, back, err)
		}
	}
	var old ColType
	if err := json.Unmarshal([]byte(`{"kind":"string"}`), &old); err != nil || old != typeString {
		t.Errorf("old string type reads as %+v, %v", old, err)
	}
	if b, _ := json.Marshal(ColType{Kind: KindString, Length: 3}); string(b) != `{"kind":"string","length":3}` {
		t.Errorf("VARCHAR(3) is stored as %s", b)
	}
}
