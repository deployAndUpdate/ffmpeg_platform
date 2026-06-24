package ffmpeg

import (
	"os"
	"path/filepath"
	"testing"

	"go_distributed_system/internal/types"
)

func TestSplitArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"whitespace", "   ", nil},
		{"simple", "-c:v libx264 -preset fast", []string{"-c:v", "libx264", "-preset", "fast"}},
		{"double quotes", `-vf "scale=1280:720"`, []string{"-vf", "scale=1280:720"}},
		{"single quotes", "-c:v 'libx264'", []string{"-c:v", "libx264"}},
		{"mixed tabs", "-c:v\tlibx264", []string{"-c:v", "libx264"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := splitArgs(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("splitArgs(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("splitArgs(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestBuildArgs(t *testing.T) {
	job := types.Job{
		InputPath:  "/data/in.mp4",
		OutputPath: "/data/out.mp4",
		FFmpegArgs: `-c:v libx264 -preset "fast"`,
	}
	got := buildArgs(job)
	want := []string{"-y", "-i", "/data/in.mp4", "-c:v", "libx264", "-preset", "fast", "/data/out.mp4"}
	if len(got) != len(want) {
		t.Fatalf("buildArgs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("buildArgs()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEnsureInputExists(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.mp4")
	if err := EnsureInputExists(missing); err == nil {
		t.Fatal("expected error for missing file")
	}

	file := filepath.Join(dir, "clip.mp4")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := EnsureInputExists(file); err != nil {
		t.Fatalf("existing file: %v", err)
	}

	if err := EnsureInputExists(dir); err == nil {
		t.Fatal("expected error when path is directory")
	}
}

func TestEnsureOutputDir(t *testing.T) {
	root := t.TempDir()

	if err := EnsureOutputDir("output.mp4"); err != nil {
		t.Fatalf("bare filename: %v", err)
	}

	out := filepath.Join(root, "nested", "dir", "out.mp4")
	if err := EnsureOutputDir(out); err != nil {
		t.Fatalf("create nested dir: %v", err)
	}
	if info, err := os.Stat(filepath.Join(root, "nested", "dir")); err != nil || !info.IsDir() {
		t.Fatalf("expected directory created: %v", err)
	}
}
