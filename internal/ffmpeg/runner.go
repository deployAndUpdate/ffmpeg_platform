package ffmpeg

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"go_distributed_system/internal/types"
)

// RunResult holds captured ffmpeg output.
type RunResult struct {
	Logs []types.JobLogEntry
	Err  error
}

// Run executes ffmpeg for the given job and captures stdout/stderr line by line.
func Run(ctx context.Context, job types.Job) RunResult {
	args := buildArgs(job)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	logs := append(
		linesToLogs("stdout", stdout),
		linesToLogs("stderr", stderr)...,
	)

	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("ffmpeg cancelled: %w", ctx.Err())
		}
		return RunResult{Logs: logs, Err: err}
	}
	return RunResult{Logs: logs}
}

func buildArgs(job types.Job) []string {
	args := []string{"-y", "-i", job.InputPath}
	args = append(args, splitArgs(job.FFmpegArgs)...)
	args = append(args, job.OutputPath)
	return args
}

func linesToLogs(stream string, buf bytes.Buffer) []types.JobLogEntry {
	var logs []types.JobLogEntry
	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		logs = append(logs, types.JobLogEntry{Stream: stream, Line: line})
	}
	return logs
}

// splitArgs splits a shell-like argument string, respecting single and double quotes.
func splitArgs(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	var args []string
	var cur strings.Builder
	var quote rune

	flush := func() {
		if cur.Len() > 0 {
			args = append(args, cur.String())
			cur.Reset()
		}
	}

	for _, r := range s {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote = r
		case r == ' ' || r == '\t':
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return args
}

// EnsureInputExists returns an error if the input file is missing.
func EnsureInputExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("input file: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("input path is a directory: %s", path)
	}
	return nil
}

// EnsureOutputDir creates the parent directory for the output file if needed.
func EnsureOutputDir(outputPath string) error {
	dir := outputPath
	if idx := strings.LastIndex(outputPath, "/"); idx >= 0 {
		dir = outputPath[:idx]
	}
	if dir == "" || dir == outputPath {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	return nil
}
