package objclient

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

type Client interface {
	// The caller should close the returned reader when done.
	Read(ctx context.Context, key string) (io.ReadCloser, error)
	// The WriteOptions can be empty for OSS clients. But caller must set the
	// Size option for S3 clients.
	Write(ctx context.Context, key string, r io.Reader, o *WriteOptions) error
	Exist(ctx context.Context, key string) (bool, error)
	Remove(ctx context.Context, keys ...string) error

	// Empty prefix will list every objects in the bucket. Otherwise, the
	// prefix should end with a "/".
	List(ctx context.Context, prefix string) ([]ObjectItem, error)
	Info(ctx context.Context, key string) (*ObjectInfo, error)
	Copy(ctx context.Context, src, dst string) error
}

type WriteOptions struct {
	// Size is required for S3 clients.
	Size int64
	// Metadata is optional. Keys should be lower case.
	Metadata map[string]string
}

type ObjectItem struct {
	Key          string
	Size         int64
	LastModified time.Time
}

type ObjectInfo struct {
	Size         int64
	LastModified time.Time
	Metadata     map[string]string
}

func stringToBool(s string, defaults bool) bool {
	if defaults {
		return s != "false"
	} else {
		return s == "true"
	}
}

// TimeoutReader will call the cancel function if Read() was blocked for about
// 30 seconds.
type TimeoutReader struct {
	r      io.Reader
	c      io.Closer
	cancel context.CancelFunc
	readed atomic.Int64
	closed atomic.Bool
}

// newTimeoutReader returns a new timeout reader.
// Caller should close it after reading.
func newTimeoutReader(r io.Reader, c io.Closer, cancel context.CancelFunc) *TimeoutReader {
	reader := new(TimeoutReader)
	reader.r = r
	reader.c = c
	reader.cancel = cancel
	go reader.timer()
	return reader
}

func (reader *TimeoutReader) Read(data []byte) (int, error) {
	n, err := reader.r.Read(data)
	reader.readed.Add(int64(n))
	return n, err
}

func (reader *TimeoutReader) Close() error {
	reader.closed.Store(true)
	reader.cancel()
	if reader.c != nil {
		reader.c.Close()
	}
	return nil
}

func (reader *TimeoutReader) timer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		if reader.closed.Load() {
			return
		}

		readed := reader.readed.Swap(0)
		if readed == 0 {
			reader.cancel()
			return
		}
	}
}
