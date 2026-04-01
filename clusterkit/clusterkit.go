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
	client    *etcd.Client
	namespace string
)

func Open(endpoints []string, prefix string) error {
	var err error

	conf := logutil.DefaultZapLoggerConfig
	conf.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)

	client, err = etcd.New(etcd.Config{
		Endpoints:   endpoints,
		DialTimeout: timeout,
		LogConfig:   &conf,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	namespace = prefix

	return nil
}

func Close() {
	client.Close()
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
