package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type logEntry struct {
	Op      string `json:"op"`
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Version uint64 `json:"version,omitempty"`
}

type WAL struct {
	file *os.File
	enc  *json.Encoder
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, enc: json.NewEncoder(f)}, nil
}

func (w *WAL) Append(e logEntry) error {
	if err := w.enc.Encode(e); err != nil {
		return err
	}
	return w.file.Sync()
}

func ReplayWAL(path string) (map[string]string, map[string]uint64, error) {
	data := make(map[string]string)
	versions := make(map[string]uint64)

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return data, versions, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var goodBytes int64
	torn := false

	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				torn = true
			}
			break
		}
		if err != nil {
			return nil, nil, err
		}

		var e logEntry
		if err := json.Unmarshal(line, &e); err != nil {
			if _, peekErr := reader.Peek(1); peekErr == io.EOF {
				torn = true
				break
			}
			return nil, nil, fmt.Errorf("corrupt WAL entry at byte %d: %w", goodBytes, err)
		}

		switch e.Op {
		case "put":
			data[e.Key] = e.Value
			versions[e.Key] = e.Version
		case "delete":
			delete(data, e.Key)
			delete(versions, e.Key)
		}
		goodBytes += int64(len(line))
	}

	if torn {
		log.Printf("WAL: damaged tail found, truncating %s to %d bytes", path, goodBytes)
		if err := os.Truncate(path, goodBytes); err != nil {
			return nil, nil, err
		}
	}
	return data, versions, nil
}
