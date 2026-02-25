package storage

import (
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"

	"puppy-db-tool-desktop/internal/model"
)

type Repository struct {
	store *Store
	data  model.AppData
}

func NewRepository(store *Store) (*Repository, error) {
	data, err := store.Load()
	if err != nil {
		return nil, err
	}
	return &Repository{store: store, data: data}, nil
}

func (r *Repository) Connections() []model.Connection {
	return slices.Clone(r.data.Connections)
}

func (r *Repository) Snippets() []model.Snippet {
	return slices.Clone(r.data.Snippets)
}

func (r *Repository) History() []model.QueryHistory {
	return slices.Clone(r.data.History)
}

func (r *Repository) UpsertConnection(conn model.Connection) (model.Connection, error) {
	now := time.Now()
	if conn.ID == "" {
		conn.ID = uuid.NewString()
		conn.CreatedAt = now
	}
	conn.UpdatedAt = now

	for i, current := range r.data.Connections {
		if current.ID == conn.ID {
			conn.CreatedAt = current.CreatedAt
			r.data.Connections[i] = conn
			return conn, r.persist()
		}
	}

	r.data.Connections = append(r.data.Connections, conn)
	return conn, r.persist()
}

func (r *Repository) DeleteConnection(id string) error {
	filtered := make([]model.Connection, 0, len(r.data.Connections))
	for _, conn := range r.data.Connections {
		if conn.ID != id {
			filtered = append(filtered, conn)
		}
	}
	r.data.Connections = filtered
	return r.persist()
}

func (r *Repository) SaveSnippet(snippet model.Snippet) (model.Snippet, error) {
	now := time.Now()
	if snippet.ID == "" {
		snippet.ID = uuid.NewString()
		snippet.CreatedAt = now
	}
	snippet.UpdatedAt = now

	for i, current := range r.data.Snippets {
		if current.ID == snippet.ID {
			snippet.CreatedAt = current.CreatedAt
			r.data.Snippets[i] = snippet
			return snippet, r.persist()
		}
	}

	r.data.Snippets = append(r.data.Snippets, snippet)
	return snippet, r.persist()
}

func (r *Repository) DeleteSnippet(id string) error {
	filtered := make([]model.Snippet, 0, len(r.data.Snippets))
	for _, item := range r.data.Snippets {
		if item.ID != id {
			filtered = append(filtered, item)
		}
	}
	r.data.Snippets = filtered
	return r.persist()
}

func (r *Repository) AppendHistory(item model.QueryHistory) error {
	if item.ID == "" {
		item.ID = uuid.NewString()
	}
	if item.ExecutedAt.IsZero() {
		item.ExecutedAt = time.Now()
	}
	r.data.History = append([]model.QueryHistory{item}, r.data.History...)
	if len(r.data.History) > 200 {
		r.data.History = r.data.History[:200]
	}
	return r.persist()
}

func (r *Repository) ConnectionByID(id string) (model.Connection, error) {
	for _, conn := range r.data.Connections {
		if conn.ID == id {
			return conn, nil
		}
	}
	return model.Connection{}, errors.New("connection not found")
}

func (r *Repository) persist() error {
	return r.store.Save(r.data)
}
