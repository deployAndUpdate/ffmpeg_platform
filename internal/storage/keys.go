package storage

import (
	"fmt"
	"path/filepath"
	"strings"
)

// InputObjectKey builds the R2 key for a job input file.
func InputObjectKey(jobID, ext string) string {
	ext = normalizeExt(ext)
	return fmt.Sprintf("jobs/%s/input.%s", jobID, ext)
}

// OutputObjectKey builds the R2 key for a job output file.
func OutputObjectKey(jobID, ext string) string {
	ext = normalizeExt(ext)
	return fmt.Sprintf("jobs/%s/output.%s", jobID, ext)
}

// ExtFromFilename returns a lowercase extension without the leading dot.
func ExtFromFilename(name string) string {
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(name)), ".")
	if ext == "" {
		return "bin"
	}
	return ext
}

func normalizeExt(ext string) string {
	ext = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ext)), ".")
	if ext == "" {
		return "bin"
	}
	return ext
}
