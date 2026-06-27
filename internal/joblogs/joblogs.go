package joblogs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"
)

const contentType = "application/x-ndjson"

type storedLine struct {
	TS     time.Time `json:"ts"`
	Stream string    `json:"stream"`
	Line   string    `json:"line"`
}

// Upload writes job logs to object storage and returns artifact metadata.
func Upload(ctx context.Context, obj storage.ObjectStorage, jobID string, attempt int, logs []types.JobLogEntry) (types.JobLogArtifactInput, error) {
	if len(logs) == 0 {
		return types.JobLogArtifactInput{}, fmt.Errorf("logs are empty")
	}
	if obj == nil {
		return types.JobLogArtifactInput{}, fmt.Errorf("object storage is not configured")
	}

	body, lines, err := encodeJSONL(logs)
	if err != nil {
		return types.JobLogArtifactInput{}, err
	}

	key := storage.LogObjectKey(jobID, attempt)
	if err := obj.UploadReader(ctx, key, bytes.NewReader(body), int64(len(body)), contentType); err != nil {
		return types.JobLogArtifactInput{}, fmt.Errorf("upload logs: %w", err)
	}

	return types.JobLogArtifactInput{
		Attempt:   attempt,
		ObjectKey: key,
		Bytes:     int64(len(body)),
		Lines:     lines,
	}, nil
}

// LoadAll reads and merges log lines from all artifacts in attempt order.
func LoadAll(ctx context.Context, obj storage.ObjectStorage, artifacts []types.JobLogArtifact) ([]types.JobLog, error) {
	if obj == nil {
		return nil, fmt.Errorf("object storage is not configured")
	}
	if len(artifacts) == 0 {
		return []types.JobLog{}, nil
	}

	sorted := append([]types.JobLogArtifact(nil), artifacts...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Attempt != sorted[j].Attempt {
			return sorted[i].Attempt < sorted[j].Attempt
		}
		return sorted[i].ID < sorted[j].ID
	})

	var out []types.JobLog
	var seq int64
	for _, art := range sorted {
		lines, err := loadObject(ctx, obj, art.ObjectKey)
		if err != nil {
			return nil, fmt.Errorf("load %q: %w", art.ObjectKey, err)
		}
		for _, line := range lines {
			seq++
			out = append(out, types.JobLog{
				ID:     seq,
				TS:     line.TS,
				Stream: line.Stream,
				Line:   line.Line,
			})
		}
	}
	return out, nil
}

func encodeJSONL(logs []types.JobLogEntry) ([]byte, int, error) {
	now := time.Now().UTC()
	var buf bytes.Buffer
	lines := 0
	for _, entry := range logs {
		if entry.Line == "" {
			continue
		}
		rec := storedLine{
			TS:     now,
			Stream: entry.Stream,
			Line:   entry.Line,
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return nil, 0, err
		}
		buf.Write(b)
		buf.WriteByte('\n')
		lines++
	}
	if lines == 0 {
		return nil, 0, fmt.Errorf("logs contain no non-empty lines")
	}
	return buf.Bytes(), lines, nil
}

func loadObject(ctx context.Context, obj storage.ObjectStorage, key string) ([]storedLine, error) {
	rc, err := obj.OpenObject(ctx, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	var lines []storedLine
	scanner := bufio.NewScanner(rc)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec storedLine
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("decode jsonl: %w", err)
		}
		if rec.TS.IsZero() {
			rec.TS = time.Now().UTC()
		}
		lines = append(lines, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

// DecodeJSONL parses JSON Lines for tests.
func DecodeJSONL(r io.Reader) ([]types.JobLog, error) {
	scanner := bufio.NewScanner(r)
	var out []types.JobLog
	var id int64
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var rec storedLine
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return nil, err
		}
		id++
		out = append(out, types.JobLog{
			ID:     id,
			TS:     rec.TS,
			Stream: rec.Stream,
			Line:   rec.Line,
		})
	}
	return out, scanner.Err()
}
