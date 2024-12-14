package crdt

import (
	"crdt/internal/util"
	"encoding/json"
	"log/slog"
	"net/http"
)

func (c *CRDT) PatchHandler(w http.ResponseWriter, r *http.Request) {
	var updates map[string]string
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	slog.Info("Patch request", "replicaID", c.origin, "updates", updates)

	c.lock.Lock()
	for key, value := range updates {
		c.incrementClock()
		opType := OpAdd
		if len(value) == 0 {
			opType = OpRemove
		}
		op := Operation{
			Key:       key,
			Value:     value,
			Timestamp: util.CopyMap(c.vector),
			Type:      opType,
			Origin:    c.origin,
		}
		c.apply(op)
	}
	slog.Info(
		"State after patch",
		"vector", c.vector,
		"timestamps", c.timestamps,
		"data", c.data,
		"history", c.history,
	)
	c.lock.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

func (c *CRDT) SyncHandler(w http.ResponseWriter, r *http.Request) {
	var incoming []Operation
	if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	slog.Info("Sync request", "replicaID", c.origin, "operations", incoming)

	c.lock.Lock()
	for _, op := range incoming {
		c.apply(op)
		c.mergeClock(op.Timestamp)
	}
	slog.Info(
		"State after sync",
		"vector", c.vector,
		"timestamps", c.timestamps,
		"data", c.data,
		"history", c.history,
	)
	c.lock.Unlock()

	w.WriteHeader(http.StatusNoContent)
}
