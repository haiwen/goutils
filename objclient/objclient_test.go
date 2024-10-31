package objclient

import (
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
		Region:      os.Getenv("s3_region"),
		HTTPS:       "true",
		Bucket:      os.Getenv("s3_bucket"),
		KeyID:       os.Getenv("s3_key_id"),
		Key:         os.Getenv("s3_key"),
		V4Signature: "true",
	})
	if err != nil {
		t.Fatal(err)
	}
	client = cli

	// We clean the "objclient/" prefix first.
	t.Run("Clean", testRemove)

	t.Run("ReadWrite", testReadWrite)
	t.Run("Exist", testExist)
	t.Run("Info", testInfo)
	t.Run("Copy", testCopy)
	t.Run("List", testList)
	t.Run("Remove", testRemove)
}

func TestOSSClient(t *testing.T) {
	cli, err := NewOSSClient(OSSConfig{
		Endpoint: "oss-" + os.Getenv("oss_region") + ".aliyuncs.com",
		Region:   os.Getenv("oss_region"),
		Bucket:   os.Getenv("oss_bucket"),
		KeyID:    os.Getenv("oss_key_id"),
		Key:      os.Getenv("oss_key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	client = cli

	// We clean the "objclient/" prefix first.
	t.Run("Remove", testRemove)

	t.Run("ReadWrite", testReadWrite)
	t.Run("Exist", testExist)
	t.Run("Info", testInfo)
	t.Run("Copy", testCopy)
	t.Run("List", testList)
	t.Run("Remove", testRemove)

}

func testReadWrite(t *testing.T) {
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

func testExist(t *testing.T) {
	exist, err := client.Exist(ctx, "objclient/test")
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatal("expect object exists")
	}

	exist, err = client.Exist(ctx, "objclient/copy")
	if err != nil {
		t.Fatal(err)
	}
	if exist {
		t.Fatal("expect object not exist")
	}
}

func testInfo(t *testing.T) {
	body := strings.NewReader("demo")
	meta := map[string]string{"foo": "bar"}

	err := client.Write(ctx, "objclient/test", body,
		&WriteOptions{Size: body.Size(), Metadata: meta},
	)
	if err != nil {
		t.Fatal(err)
	}
	info, err := client.Info(ctx, "objclient/test")
	if err != nil {
		t.Fatal(err)
	}

	if info.Size != body.Size() {
		t.Fatalf("invalid size value: %v", info.Size)
	}
	if len(info.Metadata) != 1 {
		t.Fatalf("invalid metadata: %v", info.Metadata)
	}
	if info.Metadata["foo"] != "bar" {
		t.Fatalf("invalid metadata: %v", info.Metadata)
	}
}

func testCopy(t *testing.T) {
	err := client.Copy(ctx, "objclient/test", "objclient/copy")
	if err != nil {
		t.Fatal(err)
	}

	exist, err := client.Exist(ctx, "objclient/copy")
	if err != nil {
		t.Fatal(err)
	}
	if !exist {
		t.Fatal("expect object exists")
	}
}

func testList(t *testing.T) {
	items, err := client.List(ctx, "objclient/")
	if err != nil {
		t.Fatal(err)
	}

	if len(items) != 2 {
		t.Fatalf("invalid items: %v", items)
	}
}

func testRemove(t *testing.T) {
	items, err := client.List(ctx, "objclient/")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range items {
		err := client.Remove(ctx, item.Key)
		if err != nil {
			t.Fatal(err)
		}
	}
}
