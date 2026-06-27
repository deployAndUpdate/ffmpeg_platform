package api

import (
	"errors"
	"testing"

	"go_distributed_system/internal/presets"
)

func TestResolveTranscodeSpecPreset(t *testing.T) {
	spec, err := resolveTranscodeSpec("h264_crf23", "", "mp4")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Preset != "h264_crf23" {
		t.Fatalf("preset = %q", spec.Preset)
	}
	if spec.FFmpegArgs != "-c:v libx264 -crf 23 -preset medium" {
		t.Fatalf("ffmpeg_args = %q", spec.FFmpegArgs)
	}
}

func TestResolveTranscodeSpecLegacyArgs(t *testing.T) {
	spec, err := resolveTranscodeSpec("", "-c:v libx264", "")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Preset != "" {
		t.Fatalf("preset = %q, want empty", spec.Preset)
	}
	if spec.FFmpegArgs != "-c:v libx264" {
		t.Fatalf("ffmpeg_args = %q", spec.FFmpegArgs)
	}
}

func TestResolveTranscodeSpecBothRejected(t *testing.T) {
	_, err := resolveTranscodeSpec("h264_crf23", "-c copy", "mp4")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveTranscodeSpecMissing(t *testing.T) {
	_, err := resolveTranscodeSpec("", "", "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveTranscodeSpecIncompatibleExt(t *testing.T) {
	_, err := resolveTranscodeSpec("mp3_192k", "", "mp4")
	if !errors.Is(err, presets.ErrIncompatibleOutputExt) {
		t.Fatalf("err = %v", err)
	}
}

func TestOutputExtFromPath(t *testing.T) {
	if got := outputExtFromPath("/data/out.mp4"); got != "mp4" {
		t.Fatalf("got %q", got)
	}
	if got := outputExtFromPath("/data/out"); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
