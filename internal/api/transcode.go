package api

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"go_distributed_system/internal/presets"
)

type transcodeSpec struct {
	Preset             string
	FFmpegArgs         string
	MaxDurationSeconds int
}

func resolveMaxDurationSeconds(requestSeconds int, preset string) int {
	if requestSeconds > 0 {
		return requestSeconds
	}
	if preset != "" {
		if sec := presets.MaxDurationSeconds(preset); sec > 0 {
			return sec
		}
	}
	return int(JobMaxDurationFromEnv().Seconds())
}

func resolveTranscodeSpec(preset, ffmpegArgs, outputExt string) (transcodeSpec, error) {
	preset = strings.TrimSpace(preset)
	ffmpegArgs = strings.TrimSpace(ffmpegArgs)
	outputExt = strings.TrimSpace(outputExt)

	if preset != "" && ffmpegArgs != "" {
		return transcodeSpec{}, fmt.Errorf("specify preset or ffmpeg_args, not both")
	}
	if preset == "" && ffmpegArgs == "" {
		return transcodeSpec{}, fmt.Errorf("preset or ffmpeg_args is required")
	}

	if preset != "" {
		p, err := presets.Resolve(preset)
		if err != nil {
			return transcodeSpec{}, err
		}
		if outputExt != "" {
			if err := presets.ValidateOutputExt(preset, outputExt); err != nil {
				return transcodeSpec{}, err
			}
		}
		return transcodeSpec{
			Preset:             preset,
			FFmpegArgs:         p.FFmpegArgs,
			MaxDurationSeconds: resolveMaxDurationSeconds(0, preset),
		}, nil
	}

	return transcodeSpec{
		FFmpegArgs:         ffmpegArgs,
		MaxDurationSeconds: int(JobMaxDurationFromEnv().Seconds()),
	}, nil
}

func outputExtFromPath(path string) string {
	ext := filepath.Ext(path)
	if ext == "" {
		return ""
	}
	return sanitizeOutputExt(strings.TrimPrefix(ext, "."))
}

func transcodeSpecHTTPError(err error) (int, string) {
	switch {
	case errors.Is(err, presets.ErrUnknownPreset):
		return 400, err.Error()
	case errors.Is(err, presets.ErrIncompatibleOutputExt):
		return 400, err.Error()
	default:
		return 400, err.Error()
	}
}
