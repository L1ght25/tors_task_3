package crdt_test

import (
	testutils "crdt/internal/test_utils"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	cluster := testutils.NewTestCluster(3)
	err := cluster[0].PatchServer(map[string]string{
		"1": "2",
	})
	assert.NoError(t, err)
	cluster[0].ForceSyncServer()
	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 1, len(server.GetHistory()))
		assert.Equal(t, "2", server.GetValue("1"))
	}
}

func TestSyncAllNodes(t *testing.T) {
	cluster := testutils.NewTestCluster(3)

	for i, server := range cluster {
		err := server.PatchServer(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)
	}

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 3, len(server.GetHistory()))
		assert.Equal(t, map[string]string{
			"0": "1",
			"1": "2",
			"2": "3",
		}, server.GetData())
	}
}

func TestRecovery(t *testing.T) {
	cluster := testutils.NewTestCluster(3)
	cluster[2].StopServer()

	for i, server := range cluster {
		if i == 2 {
			continue
		}

		err := server.PatchServer(map[string]string{
			fmt.Sprint(i): fmt.Sprint(i + 1),
		})
		assert.NoError(t, err)
	}

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	cluster[2].StartTestServer()

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for _, server := range cluster {
		assert.Equal(t, 2, len(server.GetHistory()))
		assert.Equal(t, map[string]string{
			"0": "1",
			"1": "2",
		}, server.GetData())
	}
}

func TestConflict(t *testing.T) {
	cluster := testutils.NewTestCluster(3)

	err := cluster[0].PatchServer(map[string]string{
		"1": "2",
	})
	assert.NoError(t, err)

	err = cluster[1].PatchServer(map[string]string{
		"1": "3",
	})
	assert.NoError(t, err)

	for _, server := range cluster {
		server.ForceSyncServer()
	}

	time.Sleep(1 * time.Second)

	for i, server := range cluster {
		if i == 0 {
			assert.Equal(t, 2, len(server.GetHistory()))
		} else if i == 1 {
			assert.Equal(t, 1, len(server.GetHistory()))
		}

		assert.Equal(t, map[string]string{
			"1": "3",
		}, server.GetData())
	}
}
