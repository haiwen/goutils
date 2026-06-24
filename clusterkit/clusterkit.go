package clusterkit

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	timeout time.Duration = time.Second * 10
)

var (
	client      *etcd.Client
	clientOwned bool
	namespace   string
)

// OpenWithClient reuses an existing etcd client. Close will not close the
// injected client; its lifecycle stays with the caller.
func OpenWithClient(c *etcd.Client, prefix string) error {
	client = c
	clientOwned = false
	namespace = prefix
	return nil
}

func Open(endpoints []string, prefix string) error {
	conf := logutil.DefaultZapLoggerConfig
	conf.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)

	c, err := etcd.New(etcd.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
		LogConfig:   &conf,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	client = c
	clientOwned = true
	namespace = prefix

	return nil
}

func Close() {
	if client != nil && clientOwned {
		client.Close()
	}
	client = nil
	clientOwned = false
	namespace = ""
}

func Delete(ctx context.Context, key string, prefix bool) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	key = namespace + key
	var opts []etcd.OpOption
	if prefix {
		opts = append(opts, etcd.WithPrefix())
	}
	_, err := client.Delete(ctx, key, opts...)
	if err != nil {
		return fmt.Errorf("failed to delete etcd key %#v: %w", key, err)
	}

	return nil
}
