//go:build integration

package testutil

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"go_distributed_system/internal/types"

	"github.com/google/uuid"
)

const sampleVideoRel = "testdata/fixtures/sample.mp4"

// SampleVideoFixturePath returns the path to the committed integration test video.
func SampleVideoFixturePath(t *testing.T) string {
	t.Helper()
	root := moduleRoot(t)
	path := filepath.Join(root, sampleVideoRel)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("sample video fixture missing at %s: %v", path, err)
	}
	return path
}

// CopySampleVideo copies the fixture into dest (parent dirs created as needed).
func CopySampleVideo(t *testing.T, dest string) {
	t.Helper()
	src := SampleVideoFixturePath(t)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatal(err)
	}
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
}

// ShortLocalMP3Job prepares a local mp3_192k transcode job using the sample fixture.
func ShortLocalMP3Job(t *testing.T) types.Job {
	t.Helper()
	workDir := t.TempDir()
	inputPath := filepath.Join(workDir, "input.mp4")
	outputPath := filepath.Join(workDir, "output.mp3")
	CopySampleVideo(t, inputPath)
	return types.Job{
		ID:              uuid.New().String(),
		InputPath:       inputPath,
		OutputPath:      outputPath,
		Preset:          "mp3_192k",
		FFmpegArgs:      "-vn -acodec libmp3lame -b:a 192k",
		Storage:         types.StorageLocal,
		LeaseGeneration: 1,
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
