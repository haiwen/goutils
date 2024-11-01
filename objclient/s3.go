package objclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

type S3Config struct {
	Endpoint         string
	Region           string
	HTTPS            string
	Bucket           string
	PathStyleRequest string
	KeyID            string
	Key              string
	V4Signature      string
	SSECKey          string
}

type S3Client struct {
	backend *minio.Client
	bucket  string
	sseckey encrypt.ServerSide
}

func NewS3Client(config S3Config) (Client, error) {
	var client S3Client

	v4Signature := stringToBool(config.V4Signature, false)

	region := config.Region
	if v4Signature && region == "" {
		region = "us-east-1"
	}

	endpoint := config.Endpoint
	if endpoint == "" {
		if region != "" {
			endpoint = "s3." + region + ".amazonaws.com"
		} else {
			endpoint = "s3.amazonaws.com"
		}
	}

	creds := credentials.NewStaticV2(config.KeyID, config.Key, "")
	if v4Signature {
		creds = credentials.NewStaticV4(config.KeyID, config.Key, "")
	}

	https := stringToBool(config.HTTPS, true)

	lookup := minio.BucketLookupDNS
	if stringToBool(config.PathStyleRequest, false) {
		lookup = minio.BucketLookupPath
	}

	if config.SSECKey != "" {
		if len(config.SSECKey) != 32 {
			return nil, errors.New("length of SSE-C key must be 32 bytes")
		}
		if !v4Signature {
			return nil, errors.New("SSE-C key requires v4 signature")
		}
		if !https {
			return nil, errors.New("SSE-C key requires https")
		}
		key, err := encrypt.NewSSEC([]byte(config.SSECKey))
		if err != nil {
			return nil, fmt.Errorf("failed to load SSE-C key: %w", err)
		}
		client.sseckey = key
	}

	backend, err := minio.New(endpoint, &minio.Options{
		Region:       region,
		Creds:        creds,
		Secure:       https,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 client: %w", err)
	}

	client.backend = backend
	client.bucket = config.Bucket

	return &client, nil
}

func (client *S3Client) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	var opts minio.GetObjectOptions
	if client.sseckey != nil {
		opts.ServerSideEncryption = client.sseckey
	}

	obj, err := client.backend.GetObject(ctx, client.bucket, key, opts)
	if err != nil {
		cancel()
		return nil, err
	}

	r := newTimeoutReader(obj, obj, cancel)
	return r, nil
}

func (client *S3Client) Write(ctx context.Context, key string, r io.Reader, o *WriteOptions) error {
	if o == nil || o.Size == 0 {
		// The minio client will consume memory heavily without knowning the size.
		return errors.New("the size option must be specified")
	}

	ctx, cancel := context.WithCancel(ctx)
	reader := newTimeoutReader(r, nil, cancel)
	defer reader.Close()

	var opts minio.PutObjectOptions
	if client.sseckey != nil {
		opts.ServerSideEncryption = client.sseckey
	}
	if len(o.Metadata) > 0 {
		opts.UserMetadata = make(map[string]string)
		for key, val := range o.Metadata {
			key = strings.ToLower(key)
			opts.UserMetadata[key] = val
		}
		opts.UserMetadata = o.Metadata
	}

	_, err := client.backend.PutObject(ctx, client.bucket, key, reader, o.Size, opts)
	if err != nil {
		return err
	}

	return nil
}

func (client *S3Client) Exist(ctx context.Context, key string) (bool, error) {
	var opts minio.StatObjectOptions
	if client.sseckey != nil {
		opts.ServerSideEncryption = client.sseckey
	}

	_, err := client.backend.StatObject(ctx, client.bucket, key, opts)
	if minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (client *S3Client) Remove(ctx context.Context, keys ...string) error {
	var (
		opts minio.RemoveObjectsOptions
		err  error
	)

	if len(keys) == 0 {
		return nil
	}
	objs := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objs <- minio.ObjectInfo{Key: key}
	}
	close(objs)
	errs := client.backend.RemoveObjects(ctx, client.bucket, objs, opts)
	for e := range errs {
		if err == nil {
			err = fmt.Errorf("failed to remove %v: %w", e.ObjectName, e.Err)
		}
	}

	return err
}

func (client *S3Client) List(ctx context.Context, prefix string) ([]ObjectItem, error) {
	var opts minio.ListObjectsOptions
	opts.Prefix = prefix
	opts.Recursive = true

	objs := client.backend.ListObjects(ctx, client.bucket, opts)

	var (
		items []ObjectItem
		err   error
	)
	for obj := range objs {
		if obj.Err != nil {
			err = obj.Err
			continue
		}

		items = append(items, ObjectItem{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified,
		})
	}
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (client *S3Client) Info(ctx context.Context, key string) (*ObjectInfo, error) {
	var opts minio.StatObjectOptions
	if client.sseckey != nil {
		opts.ServerSideEncryption = client.sseckey
	}

	stat, err := client.backend.StatObject(ctx, client.bucket, key, opts)
	if err != nil {
		return nil, err
	}

	info := &ObjectInfo{
		Size:         stat.Size,
		Metadata:     make(map[string]string),
		LastModified: stat.LastModified,
	}
	for key, val := range stat.UserMetadata {
		key = strings.ToLower(key)
		info.Metadata[key] = val
	}

	return info, nil
}

func (client *S3Client) Copy(ctx context.Context, src, dst string) error {
	srcOpts := minio.CopySrcOptions{
		Bucket: client.bucket,
		Object: src,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: client.bucket,
		Object: dst,
	}

	if client.sseckey != nil {
		srcOpts.Encryption = client.sseckey
		dstOpts.Encryption = client.sseckey
	}

	_, err := client.backend.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		return err
	}

	return nil
}
