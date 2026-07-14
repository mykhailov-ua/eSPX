package mlanalytics

import (
	"log/slog"
	"sync"
)

// ModelRegistry manages the active scorers.
type ModelRegistry struct {
	mu      sync.RWMutex
	scorers map[string]Scorer
	active  string
}

// NewModelRegistry creates a new ModelRegistry.
func NewModelRegistry() *ModelRegistry {
	return &ModelRegistry{
		scorers: make(map[string]Scorer),
	}
}

// Register registers a scorer.
func (modelRegistry *ModelRegistry) Register(scorer Scorer) {
	if scorer == nil {
		return
	}
	modelRegistry.mu.Lock()
	defer modelRegistry.mu.Unlock()
	modelRegistry.scorers[scorer.Name()] = scorer
	if modelRegistry.active == "" {
		modelRegistry.active = scorer.Name()
	}
}

// SetActive sets the active scorer name.
func (modelRegistry *ModelRegistry) SetActive(name string) error {
	modelRegistry.mu.Lock()
	defer modelRegistry.mu.Unlock()
	if _, ok := modelRegistry.scorers[name]; !ok {
		slog.Error("scorer not registered", "name", name)
		return ErrScorerNotRegistered
	}
	modelRegistry.active = name
	return nil
}

// GetActive returns the active scorer.
func (modelRegistry *ModelRegistry) GetActive() Scorer {
	modelRegistry.mu.RLock()
	defer modelRegistry.mu.RUnlock()
	return modelRegistry.scorers[modelRegistry.active]
}

// Get returns a scorer by name.
func (modelRegistry *ModelRegistry) Get(name string) Scorer {
	modelRegistry.mu.RLock()
	defer modelRegistry.mu.RUnlock()
	return modelRegistry.scorers[name]
}
