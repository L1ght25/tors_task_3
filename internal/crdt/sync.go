package crdt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

func (c *CRDT) heartbeat() {
	slog.Info("heartbeat", "replicaID", c.origin)
	for _, peer := range c.peers {
		go c.syncWith(peer)
	}

	c.heartbeatTimer = time.AfterFunc(c.heartbeatTimeout, c.heartbeat)
}

func (c *CRDT) syncWith(peer string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	slog.Info("Sync...", "replicaID", c.origin, "another_replica", peer)

	data, err := json.Marshal(c.history)
	if err != nil {
		slog.Error("cannot marshal history", "history", c.history, "error", err)
		return
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/sync", peer), "application/json", bytes.NewReader(data))
	if err != nil {
		slog.Error("cannot sync with another replica", "replicaID", c.origin, "another_replica", peer, "error", err)
		return
	}
	resp.Body.Close()
}
