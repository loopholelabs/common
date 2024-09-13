// SPDX-License-Identifier: Apache-2.0

package chunk

import (
	"context"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"

	"github.com/loopholelabs/common/pkg/pool"
)

var (
	chunkPool = pool.NewPool[Chunk, *Chunk](func() *Chunk {
		return new(Chunk)
	})
)

// Chunk manages downloading a single chunk of data from a remote server
type Chunk struct {
	// client is the S3 client to use for downloading the chunk
	client *minio.Client

	// ctx is the context to use for the download
	ctx context.Context

	// bucket is the S3 bucket to download the chunk from
	bucket string

	// key is the S3 key to download the chunk from
	key string

	// opts are the options to use for the download
	opts *minio.GetObjectOptions

	// res is the S3 response from the download
	obj *minio.Object

	// data is the data downloaded from the remote server
	data []byte

	// err is the error that occurred while downloading the chunk
	err error

	// wg is the wait group used to wait for the chunk to finish downloading
	wg *sync.WaitGroup
}

func GetChunk(client *minio.Client, ctx context.Context, offset int64, size int64, bucket string, key string) (*Chunk, error) {
	c := chunkPool.Get()
	c.client = client
	c.ctx = ctx
	c.bucket = bucket
	c.key = key

	c.opts = new(minio.GetObjectOptions)
	err := c.opts.SetRange(offset, offset+size-1)
	if err != nil {
		return nil, err
	}

	c.wg = new(sync.WaitGroup)
	c.wg.Add(1)
	go c.do()
	return c, nil
}

func ReturnChunk(c *Chunk) {
	chunkPool.Put(c)
}

func (c *Chunk) do() {
	c.obj, c.err = c.client.GetObject(c.ctx, c.bucket, c.key, *c.opts)
	if c.err == nil {
		c.data, c.err = io.ReadAll(c.obj)
		_ = c.obj.Close()
	}
	c.wg.Done()
}

func (c *Chunk) Wait() ([]byte, error) {
	c.wg.Wait()
	return c.data, c.err
}

func (c *Chunk) Reset() {
	c.client = nil
	c.ctx = nil
	c.opts = nil
	c.obj = nil
	c.data = nil
	c.err = nil
	c.wg = nil
}

func (c *Chunk) Return() {
	ReturnChunk(c)
}
