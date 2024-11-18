package objclient

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type OSSConfig struct {
	Endpoint string
	Region   string
	HTTPS    string
	Bucket   string
	KeyID    string
	Key      string
}

type OSSClient struct {
	bucket *oss.Bucket
}

func NewOSSClient(config OSSConfig) (Client, error) {
	var client OSSClient

	region := config.Region

	endpoint := config.Endpoint
	if endpoint == "" && region != "" {
		endpoint = "oss-" + region + ".aliyuncs.com"
	}
	uri := url.URL{Host: endpoint}

	https := stringToBool(config.HTTPS, true)
	if https {
		uri.Scheme = "https"
	} else {
		uri.Scheme = "http"
	}

	backend, err := oss.New(uri.String(), config.KeyID, config.Key)
	if err != nil {
		return nil, err
	}
	bucket, err := backend.Bucket(config.Bucket)
	if err != nil {
		return nil, err
	}

	client.bucket = bucket

	return &client, nil
}

func (client *OSSClient) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	return client.bucket.GetObject(key, oss.WithContext(ctx))
}

func (client *OSSClient) Write(ctx context.Context, key string, r io.Reader, o *WriteOptions) error {
	var opts []oss.Option
	opts = append(opts, oss.WithContext(ctx))

	if o != nil && len(o.Metadata) > 0 {
		for key, val := range o.Metadata {
			key = strings.ToLower(key)
			opts = append(opts, oss.Meta(key, val))
		}
	}

	return client.bucket.PutObject(key, io.NopCloser(r), opts...)
}

func (client *OSSClient) Exist(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	return client.bucket.IsObjectExist(key, oss.WithContext(ctx))
}

func (client *OSSClient) Remove(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	_, err := client.bucket.DeleteObjects(keys, oss.WithContext(ctx))
	return err
}

func (client *OSSClient) List(ctx context.Context, prefix string) ([]ObjectItem, error) {
	var opts []oss.Option
	opts = append(opts, oss.WithContext(ctx))
	opts = append(opts, oss.Prefix(prefix))
	opts = append(opts, oss.MaxKeys(1000))

	var (
		items []ObjectItem
		token string
	)
	for {
		o := append(opts, oss.ContinuationToken(token))
		list, err := client.bucket.ListObjectsV2(o...)
		if err != nil {
			return nil, err
		}
		for _, obj := range list.Objects {
			items = append(items, ObjectItem{
				Key:          obj.Key,
				Size:         obj.Size,
				LastModified: obj.LastModified,
			})
		}

		if !list.IsTruncated {
			break
		}
		token = list.NextContinuationToken
	}

	return items, nil
}

func (client *OSSClient) Info(ctx context.Context, key string) (*ObjectInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	header, err := client.bucket.GetObjectDetailedMeta(key, oss.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	var info ObjectInfo

	info.Size, err = strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}

	info.LastModified, err = time.Parse(http.TimeFormat, header.Get("Last-Modified"))
	if err != nil {
		return nil, err
	}

	info.Metadata = make(map[string]string)
	for key := range header {
		if !strings.HasPrefix(key, "X-Oss-Meta-") {
			continue
		}
		k := strings.TrimPrefix(key, "X-Oss-Meta-")
		info.Metadata[strings.ToLower(k)] = header.Get(key)
	}

	return &info, nil
}

func (client *OSSClient) Copy(ctx context.Context, src, dst string) error {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	_, err := client.bucket.CopyObject(src, dst, oss.WithContext(ctx))
	return err
}
