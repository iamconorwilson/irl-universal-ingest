package auth

import "testing"

func TestIsAllowed(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		allowedPaths []string
		expected     bool
	}{
		{
			name:         "empty allowed list allows all",
			path:         "/live",
			allowedPaths: []string{},
			expected:     true,
		},
		{
			name:         "exact match",
			path:         "/live",
			allowedPaths: []string{"/live", "/stream"},
			expected:     true,
		},
		{
			name:         "match without leading slash in input",
			path:         "live",
			allowedPaths: []string{"/live"},
			expected:     true,
		},
		{
			name:         "match without leading slash in allowed",
			path:         "/live",
			allowedPaths: []string{"live"},
			expected:     true,
		},
		{
			name:         "match with query parameters",
			path:         "/live?key=secret",
			allowedPaths: []string{"/live"},
			expected:     true,
		},
		{
			name:         "reject non-matching path",
			path:         "/unknown",
			allowedPaths: []string{"/live", "/stream"},
			expected:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAllowed(tt.path, tt.allowedPaths)
			if result != tt.expected {
				t.Errorf("IsAllowed(%q, %v) = %v; want %v", tt.path, tt.allowedPaths, result, tt.expected)
			}
		})
	}
}
