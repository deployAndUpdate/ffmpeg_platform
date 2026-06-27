package presets

import (
	"errors"
	"testing"
)

func TestResolve(t *testing.T) {
	p, err := Resolve("h264_crf23")
	if err != nil {
		t.Fatal(err)
	}
	if p.FFmpegArgs != "-c:v libx264 -crf 23 -preset medium" {
		t.Fatalf("ffmpeg args = %q", p.FFmpegArgs)
	}

	_, err = Resolve("missing")
	if !errors.Is(err, ErrUnknownPreset) {
		t.Fatalf("err = %v, want ErrUnknownPreset", err)
	}
}

func TestList(t *testing.T) {
	items := List()
	if len(items) != len(registry) {
		t.Fatalf("len = %d, want %d", len(items), len(registry))
	}
	for i := 1; i < len(items); i++ {
		if items[i].ID <= items[i-1].ID {
			t.Fatalf("not sorted: %q after %q", items[i].ID, items[i-1].ID)
		}
	}
}

func TestValidateOutputExt(t *testing.T) {
	if err := ValidateOutputExt("mp3_192k", "mp3"); err != nil {
		t.Fatalf("mp3 should be allowed: %v", err)
	}
	if err := ValidateOutputExt("mp3_192k", ".MP3"); err != nil {
		t.Fatalf(".MP3 should be allowed: %v", err)
	}

	err := ValidateOutputExt("mp3_192k", "mp4")
	if !errors.Is(err, ErrIncompatibleOutputExt) {
		t.Fatalf("err = %v, want ErrIncompatibleOutputExt", err)
	}

	err = ValidateOutputExt("unknown", "mp3")
	if !errors.Is(err, ErrUnknownPreset) {
		t.Fatalf("err = %v, want ErrUnknownPreset", err)
	}
}
