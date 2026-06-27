package joblogs

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"go_distributed_system/internal/storage"
	"go_distributed_system/internal/types"
)

func TestUploadAndLoadAll(t *testing.T) {
	obj := storage.NewMemory("test")
	logs := []types.JobLogEntry{
		{Stream: "stdout", Line: "frame=100"},
		{Stream: "stderr", Line: "warning"},
	}

	meta, err := Upload(context.Background(), obj, "job-1", 2, logs)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ObjectKey != "jobs/job-1/logs/attempt-2.jsonl" {
		t.Fatalf("object_key = %q", meta.ObjectKey)
	}
	if meta.Lines != 2 || meta.Bytes <= 0 {
		t.Fatalf("meta = %+v", meta)
	}

	loaded, err := LoadAll(context.Background(), obj, []types.JobLogArtifact{
		{ID: 2, Attempt: 2, ObjectKey: meta.ObjectKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 2 {
		t.Fatalf("len = %d", len(loaded))
	}
	if loaded[1].Line != "warning" {
		t.Fatalf("line = %q", loaded[1].Line)
	}
}

func TestEncodeDecodeJSONL(t *testing.T) {
	logs := []types.JobLogEntry{{Stream: "stderr", Line: "err: " + strings.Repeat("x", 10)}}
	body, n, err := encodeJSONL(logs)
	if err != nil || n != 1 {
		t.Fatalf("encode: n=%d err=%v", n, err)
	}
	decoded, err := DecodeJSONL(bytes.NewReader(body))
	if err != nil || len(decoded) != 1 || decoded[0].Line != logs[0].Line {
		t.Fatalf("decode: %+v err=%v", decoded, err)
	}
}
