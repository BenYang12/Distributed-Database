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
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
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

func ReplayWAL(path string) (map[string]string, error) {
	data := make(map[string]string)

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return data, nil
	}
	if err != nil {
		return nil, err
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
			return nil, err
		}

		var e logEntry
		if err := json.Unmarshal(line, &e); err != nil {
			if _, peekErr := reader.Peek(1); peekErr == io.EOF {
				torn = true
				break
			}
			return nil, fmt.Errorf("corrupt WAL entry at byte %d: %w", goodBytes, err)
		}

		switch e.Op {
		case "put":
			data[e.Key] = e.Value
		case "delete":
			delete(data, e.Key)
		}
		goodBytes += int64(len(line))
	}

	if torn {
		log.Printf("WAL: damaged tail found, truncating %s to %d bytes", path, goodBytes)
		if err := os.Truncate(path, goodBytes); err != nil {
			return nil, err
		}
	}
	return data, nil
}
