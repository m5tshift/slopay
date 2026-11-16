package storage

import (
	"fmt"
	"os"
	"strings"
)

// SecretFile is the path of a mounted secret. The content is read on every call, never cached,
// because the file changes when the password is rotated.
type SecretFile string

// Read returns the file content without surrounding whitespace.
func (f SecretFile) Read() (string, error) {
	if f == "" {
		return "", fmt.Errorf("storage: secret file path is empty")
	}
	b, err := os.ReadFile(string(f))
	if err != nil {
		// The error carries the path, not the content.
		return "", fmt.Errorf("storage: read secret file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}
