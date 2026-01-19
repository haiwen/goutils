package objclient

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

var (
	ctx    context.Context = context.Background()
	client Client
)

func TestS3Client(t *testing.T) {
	cli, err := NewS3Client(S3Config{
		Endpoint: os.Getenv("S3_HOST"),
		Region:   os.Getenv("S3_AWS_REGION"),
		HTTPS:    "true",
		//HTTPS:            "false",
		Bucket:      os.Getenv("MD_S3_BUCKET"),
		KeyID:       os.Getenv("S3_KEY_ID"),
		Key:         os.Getenv("S3_KEY"),
		V4Signature: "true",
		//PathStyleRequest: "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	client = cli

	t.Run("ReadWrite1", testReadWrite1)
	t.Run("ReadWrite2", testReadWrite2)
}

func testReadWrite1(t *testing.T) {
	body := strings.NewReader("demo")

	err := client.Write(ctx, "objclient/test", body,
		&WriteOptions{Size: body.Size()},
	)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.Read(ctx, "objclient/test")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "demo" {
		t.Fatalf("invalid data from Read(): %q", data)
	}
}

func testReadWrite2(t *testing.T) {
	d := make([]byte, 20*1024*1024)
	body := bytes.NewReader(d)

	err := client.Write(ctx, "objclient/test", body,
		&WriteOptions{Size: body.Size()},
	)
	if err != nil {
		t.Fatal(err)
	}
	r, err := client.Read(ctx, "objclient/test")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
}
