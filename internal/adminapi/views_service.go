package adminapi

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrViewNotFound = errors.New("view not found")
)

// Service will own PG CRUD for saved_report_views (M6 ADM-W5).
type Service struct {
	mu    sync.RWMutex
	views map[string]SavedViewDTO
}

// NewService returns a views service stub.
func NewService() *Service {
	return &Service{
		views: make(map[string]SavedViewDTO),
	}
}

func (s *Service) CreateView(req CreateViewRequest, ownerID string) SavedViewDTO {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.New().String()
	now := time.Now().UTC().Format(time.RFC3339)
	view := SavedViewDTO{
		ID:         id,
		OwnerID:    ownerID,
		CustomerID: req.CustomerID,
		Name:       req.Name,
		ReportKey:  req.ReportKey,
		Spec:       req.Spec,
		IsShared:   req.IsShared,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.views[id] = view
	return view
}

func (s *Service) GetView(id string) (SavedViewDTO, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, ok := s.views[id]
	if !ok {
		return SavedViewDTO{}, ErrViewNotFound
	}
	return view, nil
}

func (s *Service) ListView(customerID string) []SavedViewDTO {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []SavedViewDTO
	for _, v := range s.views {
		if v.CustomerID == customerID {
			list = append(list, v)
		}
	}
	return list
}

func (s *Service) UpdateView(id string, req UpdateViewRequest) (SavedViewDTO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	view, ok := s.views[id]
	if !ok {
		return SavedViewDTO{}, ErrViewNotFound
	}

	view.Name = req.Name
	view.ReportKey = req.ReportKey
	view.Spec = req.Spec
	view.IsShared = req.IsShared
	view.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	s.views[id] = view
	return view, nil
}

func (s *Service) DeleteView(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.views[id]; !ok {
		return ErrViewNotFound
	}
	delete(s.views, id)
	return nil
}
