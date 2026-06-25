package presets

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

var (
	ErrUnknownPreset         = errors.New("unknown preset")
	ErrIncompatibleOutputExt = errors.New("output extension incompatible with preset")
)

// Preset is a named transcode profile with fixed ffmpeg arguments (without -i and output path).
type Preset struct {
	ID          string
	Description string
	FFmpegArgs  string
	OutputExts  []string
}

var registry = map[string]Preset{
	"h264_crf23": {
		ID:          "h264_crf23",
		Description: "H.264 video, CRF 23, medium x264 preset",
		FFmpegArgs:  "-c:v libx264 -crf 23 -preset medium",
		OutputExts:  []string{"mp4", "mkv"},
	},
	"h264_crf28": {
		ID:          "h264_crf28",
		Description: "H.264 video, CRF 28, fast x264 preset",
		FFmpegArgs:  "-c:v libx264 -crf 28 -preset fast",
		OutputExts:  []string{"mp4", "mkv"},
	},
	"copy_video": {
		ID:          "copy_video",
		Description: "Remux without re-encoding",
		FFmpegArgs:  "-c copy",
		OutputExts:  []string{"mp4", "mkv", "webm"},
	},
	"mp3_192k": {
		ID:          "mp3_192k",
		Description: "Extract audio as MP3 192 kbps",
		FFmpegArgs:  "-vn -acodec libmp3lame -b:a 192k",
		OutputExts:  []string{"mp3"},
	},
	"mp3_128k": {
		ID:          "mp3_128k",
		Description: "Extract audio as MP3 128 kbps",
		FFmpegArgs:  "-vn -acodec libmp3lame -b:a 128k",
		OutputExts:  []string{"mp3"},
	},
	"aac_128k": {
		ID:          "aac_128k",
		Description: "Extract audio as AAC 128 kbps",
		FFmpegArgs:  "-vn -c:a aac -b:a 128k",
		OutputExts:  []string{"m4a", "mp4"},
	},
}

// Resolve returns a preset by id.
func Resolve(id string) (Preset, error) {
	id = strings.TrimSpace(id)
	p, ok := registry[id]
	if !ok {
		return Preset{}, fmt.Errorf("%w: %q", ErrUnknownPreset, id)
	}
	return p, nil
}

// List returns all presets sorted by id.
func List() []Preset {
	out := make([]Preset, 0, len(registry))
	for _, p := range registry {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}

// ValidateOutputExt checks that ext is allowed for the preset.
func ValidateOutputExt(id, ext string) error {
	p, err := Resolve(id)
	if err != nil {
		return err
	}
	ext = normalizeExt(ext)
	for _, allowed := range p.OutputExts {
		if ext == allowed {
			return nil
		}
	}
	return fmt.Errorf("%w: preset %q does not support %q (allowed: %s)",
		ErrIncompatibleOutputExt, id, ext, strings.Join(p.OutputExts, ", "))
}

// MaxDurationSeconds returns a suggested max runtime for a preset, or 0 if unknown.
func MaxDurationSeconds(id string) int {
	id = strings.TrimSpace(id)
	d, ok := maxDurationSeconds[id]
	if !ok {
		return 0
	}
	return int(d.Seconds())
}

var maxDurationSeconds = map[string]time.Duration{
	"h264_crf23": 4 * time.Hour,
	"h264_crf28": 4 * time.Hour,
	"copy_video": 30 * time.Minute,
	"mp3_192k":   1 * time.Hour,
	"mp3_128k":   1 * time.Hour,
	"aac_128k":   1 * time.Hour,
}

func normalizeExt(ext string) string {
	return strings.TrimPrefix(strings.ToLower(strings.TrimSpace(ext)), ".")
}
