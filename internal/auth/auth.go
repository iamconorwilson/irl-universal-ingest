package auth

import (
	"net/url"
	"strings"
)

// NormalizePath cleans and strips query parameters or trailing slashes for path matching.
func NormalizePath(rawPath string) string {
	parsed, err := url.Parse(rawPath)
	if err == nil && parsed.Path != "" {
		rawPath = parsed.Path
	}

	trimmed := strings.TrimSpace(rawPath)
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

// IsAllowed verifies whether the provided path is permitted by the allowedPaths list.
// An empty allowedPaths list permits all paths.
func IsAllowed(path string, allowedPaths []string) bool {
	if len(allowedPaths) == 0 {
		return true
	}

	normalizedInput := NormalizePath(path)
	for _, allowed := range allowedPaths {
		if NormalizePath(allowed) == normalizedInput {
			return true
		}
	}

	return false
}
