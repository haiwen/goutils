package clusterkit

import (
	"context"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Mutex struct {
	mutex *concurrency.Mutex
}

func Lock(ctx context.Context, key string) (*Mutex, error) {
	ctx = etcd.WithRequireLeader(ctx)
	key = fmt.Sprintf("%s/mutex/%s", namespace, key)

	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(timeout, cancel)
	session, err := concurrency.NewSession(client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(3),
	)
	mutex := concurrency.NewMutex(session, key)
	timer.Stop()
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd session: %w", err)
	}

	err = mutex.Lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to lock etcd mutex: %w", err)
	}

	return &Mutex{mutex: mutex}, nil
}

func (m *Mutex) Unlock() error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := m.mutex.Unlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to unlock etcd mutex: %w", err)
	}
	return nil
}
