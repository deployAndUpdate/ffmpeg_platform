package storage

import "testing"

func TestInputObjectKey(t *testing.T) {
	t.Parallel()
	got := InputObjectKey("abc-123", "MP4")
	want := "jobs/abc-123/input.mp4"
	if got != want {
		t.Fatalf("InputObjectKey() = %q, want %q", got, want)
	}
}

func TestOutputObjectKey(t *testing.T) {
	t.Parallel()
	got := OutputObjectKey("abc-123", ".mp3")
	want := "jobs/abc-123/output.mp3"
	if got != want {
		t.Fatalf("OutputObjectKey() = %q, want %q", got, want)
	}
}

func TestExtFromFilename(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"video", "clip.mp4", "mp4"},
		{"upper", "CLIP.MP4", "mp4"},
		{"no ext", "clip", "bin"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ExtFromFilename(tt.in); got != tt.want {
				t.Fatalf("ExtFromFilename(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
