package storage

import (
	"errors"
	"sync"
)

type KVEngine struct {
	mu    sync.RWMutex
	store map[string]string
	wal   *WAL
}

func NewKVEngine(walPath string) (*KVEngine, error) {
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	engine := &KVEngine{
		store: make(map[string]string),
		wal:   wal,
	}

	// Crash Recovery: Replay the Write-Ahead Log
	entries, err := wal.ReadAll()
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		engine.store[entry.Key] = entry.Value
	}

	return engine, nil
}

func (e *KVEngine) Put(key, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Write to Disk (WAL) first for durability
	if err := e.wal.Append(key, value); err != nil {
		return err
	}

	// 2. Update Memory
	e.store[key] = value
	return nil
}

func (e *KVEngine) Get(key string) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, exists := e.store[key]
	if !exists {
		return "", errors.New("key not found")
	}
	return val, nil
}