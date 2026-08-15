package storage

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type LogEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// NewWAL opens or creates the Write-Ahead Log file.
func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: file}, nil
}

// Append writes a new entry to the log and flushes it to disk.
func (w *WAL) Append(key, value string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := LogEntry{Key: key, Value: value}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return err
	}
	
	// fsync guarantees the data is physically written to the drive
	return w.file.Sync()
}

// ReadAll recovers the state from the disk on startup.
func (w *WAL) ReadAll() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var entries []LogEntry
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}