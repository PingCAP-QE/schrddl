package main

import "testing"

func TestExtractIndexName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "standard ADD INDEX statement",
			input:    "alter table sbtest1 add index idx_k(k)",
			expected: "idx_k",
		},
		{
			name:     "ADD INDEX with multiple columns",
			input:    "alter table sbtest1 add index idx_k(id, k)",
			expected: "idx_k",
		},
		{
			name:     "ADD INDEX with three columns",
			input:    "alter table sbtest1 add index idx_k(c, id, k)",
			expected: "idx_k",
		},
		{
			name:     "ADD UNIQUE KEY statement",
			input:    "alter table sbtest1 add unique key idx_name(c)",
			expected: "idx_name",
		},
		{
			name:     "ADD UNIQUE KEY with multiple columns",
			input:    "alter table sbtest1 add unique key idx_name(id, c)",
			expected: "idx_name",
		},
		{
			name:     "uppercase ADD INDEX",
			input:    "ALTER TABLE sbtest1 ADD INDEX IDX_K(k)",
			expected: "IDX_K",
		},
		{
			name:     "mixed case",
			input:    "Alter Table sbtest1 Add Index idx_k(k)",
			expected: "idx_k",
		},
		{
			name:     "uppercase ADD UNIQUE KEY",
			input:    "ALTER TABLE sbtest1 ADD UNIQUE KEY IDX_NAME(c)",
			expected: "IDX_NAME",
		},
		{
			name:     "with extra spaces",
			input:    "alter table sbtest1 add  index  idx_k  (k)",
			expected: "idx_k",
		},
		{
			name:     "index name with underscore",
			input:    "alter table sbtest1 add index idx_test_name(id)",
			expected: "idx_test_name",
		},
		{
			name:     "index name with numbers",
			input:    "alter table sbtest1 add index idx_123(id)",
			expected: "idx_123",
		},
		{
			name:     "fallback method when regex doesn't match",
			input:    "alter table sbtest1 add index idx_k",
			expected: "idx_k",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "invalid input",
			input:    "invalid sql statement",
			expected: "",
		},
		{
			name:     "only ADD keyword",
			input:    "add index",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractIndexName(tt.input)
			if result != tt.expected {
				t.Errorf("extractIndexName(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}
