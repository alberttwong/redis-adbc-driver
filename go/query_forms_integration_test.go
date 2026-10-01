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

// Integration tests for subquery forms. They use the harness of
// sql_integration_test.go and are skipped when REDIS_URI is unset.

import (
	"testing"
)

// setupQueryForms creates the tables of these tests:
//
//	it_qf_g      (id, k, v) with a NULL k and a NULL v
//	it_qf_g2     (k, id, w), sharing k and id with it_qf_g
func (h *sqlHarness) setupQueryForms() {
	h.t.Helper()
	tables := []string{"it_qf_g", "it_qf_g2"}
	drop := func() {
		for _, t := range tables {
			h.exec("DROP TABLE IF EXISTS " + t)
		}
	}
	drop()
	h.t.Cleanup(drop)
	h.exec("CREATE TABLE it_qf_g (id INTEGER NOT NULL, k VARCHAR, v INTEGER)")
	h.exec("INSERT INTO it_qf_g VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', NULL), (5, NULL, 50)")
	h.exec("CREATE TABLE it_qf_g2 (k VARCHAR, id INTEGER, w INTEGER)")
	h.exec("INSERT INTO it_qf_g2 VALUES ('a', 1, 100), ('b', 3, 300), ('c', 9, 900)")
}

// In a correlated subquery, a condition on outer columns only is a constant,
// not an index filter on the inner table's column of the same name.
func TestSQLSubqueryOuterOnlyCondition(t *testing.T) {
	h := newSQLHarness(t)
	h.setupQueryForms()

	h.expectRows(`SELECT id, (SELECT COUNT(*) FROM it_qf_g2 x WHERE g.id = 3), (SELECT COUNT(*) FROM it_qf_g2 x WHERE g.id > 2)
		FROM it_qf_g g ORDER BY id`,
		"1|0|0", "2|0|0", "3|3|3", "4|0|3", "5|0|3")
	h.expectRows(`SELECT id FROM it_qf_g g WHERE NOT EXISTS (SELECT 1 FROM it_qf_g2 x WHERE g.id < 3 AND x.w > 100) ORDER BY id`,
		"3", "4", "5")
}
