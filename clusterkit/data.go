package clusterkit

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

type ClusterNodes struct {
	Nodes []ClusterNode `json:"nodes"`
}

type ClusterNode struct {
	ID  int    `json:"id"`
	URL string `json:"url"`
}

func GetClusterNodes(ctx context.Context) (*ClusterNodes, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	key := fmt.Sprintf("%s/cluster/nodes", namespace)
	rsp, err := client.Get(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get etcd key %q: %w", key, err)
	} else if len(rsp.Kvs) == 0 {
		return &ClusterNodes{}, 0, nil
	}

	var nodes ClusterNodes
	err = json.Unmarshal(rsp.Kvs[0].Value, &nodes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal %q: %w", key, err)
	}

	return &nodes, rsp.Header.Revision, nil
}

func SetClusterNode(nodes *ClusterNodes) error {
	data, err := json.Marshal(nodes)
	if err != nil {
		return fmt.Errorf("failed to marshal cluster nodes: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key := fmt.Sprintf("%s/cluster/nodes", namespace)
	_, err = client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to put etcd key %q: %w", key, err)
	}

	return nil
}

// Heartbeat represents a node heartbeat stored in etcd.
type Heartbeat struct {
	NodeID int
}

// ListHeartbeats fetches heartbeat entries from etcd under the configured prefix.
// It parses node ID from the key (expected as "<prefix>/cluster/hb/<nodeID>").
//
// Returns a non-nil slice (possibly empty) or an error.
func ListHeartbeats(ctx context.Context) ([]Heartbeat, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	prefix := fmt.Sprintf("%s/cluster/hb/", namespace)
	rsp, err := client.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get etcd key %q: %w", prefix, err)
	}
	if len(rsp.Kvs) == 0 {
		return []Heartbeat{}, nil
	}

	var hbs []Heartbeat
	for _, kv := range rsp.Kvs {
		var hb Heartbeat

		key := string(kv.Key)
		if !strings.HasPrefix(key, prefix) {
			return nil, fmt.Errorf("failed to parse etcd key %q: %w", key, err)
		}
		hb.NodeID, err = strconv.Atoi(strings.TrimPrefix(key, prefix))
		if err != nil {
			return nil, fmt.Errorf("failed to parse etcd key %q: %w", key, err)
		}

		hbs = append(hbs, hb)
	}

	return hbs, nil
}

func KeepHeartbeat(ctx context.Context, nodeID int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timer := time.AfterFunc(timeout, cancel)
	defer timer.Stop()

	lease, err := client.Grant(ctx, 180)
	if err != nil {
		return fmt.Errorf("failed to grant etcd lease: %w", err)
	}

	key := fmt.Sprintf("%s/cluster/hb/%d", namespace, nodeID)
	_, err = client.Put(ctx, key, "true", etcd.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to put etcd key %q: %w", key, err)
	}

	alive, err := client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to put etcd key %q: %w", key, err)
	}
	timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-alive:
			if !ok {
				return fmt.Errorf("failed to keep lease")
			}
		}
	}
}

// Assign represents a mapping from a prefix (two-hex-digit string) to a node.
// Prefix is derived from the key, NodeID comes from the JSON value.
type Assign struct {
	Prefix string `json:"-"`
	NodeID int    `json:"node_id"`
}

func ListAssigns(ctx context.Context) ([]Assign, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	prefix := fmt.Sprintf("%s/assign/", namespace)
	rsp, err := client.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get etcd key %q: %w", prefix, err)
	} else if len(rsp.Kvs) == 0 {
		return []Assign{}, rsp.Header.Revision, nil
	}

	var assigns []Assign
	for _, kv := range rsp.Kvs {
		var assign Assign

		key := string(kv.Key)
		if !strings.HasPrefix(key, prefix) {
			return nil, 0, fmt.Errorf("failed to parse etcd key %q: %w", key, err)
		}
		assign.Prefix = strings.TrimPrefix(key, prefix)
		err = json.Unmarshal(kv.Value, &assign)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal etcd key %q: %w", key, err)
		}

		assigns = append(assigns, assign)
	}

	return assigns, rsp.Header.Revision, nil
}

func SetAssigns(assigns []Assign) error {
	var ops []etcd.Op
	for _, assign := range assigns {
		key := fmt.Sprintf("%s/assign/%s", namespace, assign.Prefix)
		data, err := json.Marshal(assign)
		if err != nil {
			return fmt.Errorf("failed to marshal assign: %w", err)
		}
		ops = append(ops, etcd.OpPut(key, string(data)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Etcd transaction has a limit on the number of operations, which is 128
	// by default.
	for chunk := range slices.Chunk(ops, 128) {
		_, err := client.Txn(ctx).Then(chunk...).Commit()
		if err != nil {
			return fmt.Errorf("failed to put etcd key %q: %w", chunk[0].KeyBytes(), err)
		}
	}

	return nil
}
