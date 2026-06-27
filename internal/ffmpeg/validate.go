package ffmpeg

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"time"
)

const minOutputBytes = 256

// OutputLooksValid reports whether path looks like a complete media file.
func OutputLooksValid(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() || info.Size() < minOutputBytes {
		return false
	}
	return probeReadable(path)
}

func probeReadable(path string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "csv=p=0",
		path,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}
