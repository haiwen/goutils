package clusterkit

import (
	"context"
	"fmt"
	"math"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func WatchClusterNodes(ctx context.Context, rev int64) Iter[int64] {
	key := fmt.Sprintf("%s/cluster/nodes", namespace)
	return WatchUpdates(ctx, rev, key, false)
}

func WatchAssigns(ctx context.Context, rev int64) Iter[int64] {
	key := fmt.Sprintf("%s/assign/", namespace)
	return WatchUpdates(ctx, rev, key, true)
}

func WatchUpdates(ctx context.Context, rev int64, key string, prefix bool) Iter[int64] {
	return NewIter(func(yield func(int64) bool) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctx = etcd.WithRequireLeader(ctx)
		opts := []etcd.OpOption{etcd.WithRev(rev + 1)}
		if prefix {
			opts = append(opts, etcd.WithPrefix())
		}

		timer := time.AfterFunc(timeout, cancel)
		watch := client.Watch(ctx, key, opts...)
		session, err := concurrency.NewSession(client,
			concurrency.WithContext(ctx),
			concurrency.WithTTL(3),
		)
		timer.Stop()
		if err != nil {
			return fmt.Errorf("failed to watch etcd: %w", err)
		}
		defer session.Close()

		timer = time.NewTimer(time.Duration(math.MaxInt64))
		defer timer.Stop()
		var rev int64
		for {
			select {
			case <-ctx.Done():
				return nil

			case <-session.Done():
				return fmt.Errorf("etcd session closed")

			case item, ok := <-watch:
				if !ok {
					return fmt.Errorf("etcd watch channel closed")
				}
				if item.Err() != nil {
					return fmt.Errorf("failed to watch etcd: %w", item.Err())
				}
				if len(item.Events) == 0 {
					continue
				}
				rev = item.Header.Revision

				// Because etcd imposes a limit on the number of operations per
				// transaction, a single update may be divided across multiple
				// transactions. A timer is used to batch these transactions
				// together.
				timer.Reset(time.Second)

			case <-timer.C:
				if !yield(rev) {
					return nil
				}
			}
		}
	})
}

// WatchEvents watches every etcd event after rev without batching.
func WatchEvents(ctx context.Context, rev int64, key string, prefix bool) Iter[*etcd.Event] {
	return NewIter(func(yield func(*etcd.Event) bool) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		ctx = etcd.WithRequireLeader(ctx)
		opts := []etcd.OpOption{etcd.WithRev(rev + 1)}
		if prefix {
			opts = append(opts, etcd.WithPrefix())
		}

		timer := time.AfterFunc(timeout, cancel)
		watch := client.Watch(ctx, key, opts...)
		session, err := concurrency.NewSession(client,
			concurrency.WithContext(ctx),
			concurrency.WithTTL(3),
		)
		timer.Stop()
		if err != nil {
			return fmt.Errorf("failed to watch etcd: %w", err)
		}
		defer session.Close()

		for {
			select {
			case <-ctx.Done():
				return nil

			case <-session.Done():
				return fmt.Errorf("etcd session closed")

			case item, ok := <-watch:
				if !ok {
					return fmt.Errorf("etcd watch channel closed")
				}
				if item.Err() != nil {
					return fmt.Errorf("failed to watch etcd: %w", item.Err())
				}
				for _, event := range item.Events {
					if !yield(event) {
						return nil
					}
				}
			}
		}
	})
}
