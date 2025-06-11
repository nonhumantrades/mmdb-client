package client

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nonhumantrades/mmdb-client/crypto"
	"github.com/nonhumantrades/mmdb-proto/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcwire"
)

type Config struct {
	// host:port
	Address string
	// optional tls config
	TLSConfig *tls.Config
	// dial / call timeout
	// default = 10 s
	Timeout time.Duration
	// separate connections
	// default = 8
	PoolSize int
	// wait before redial
	// default = 2 s
	ReconnectInterval time.Duration
	// retries across pool
	// default = PoolSize
	MaxRetriesPerCall int
	// DRPC receive buffer
	// default = 512 MiB
	MaxBufferBytes int
}

type conn struct {
	mu     sync.RWMutex
	conn   *drpcconn.Conn
	client proto.DRPCMMDBClient
	cfg    *Config
}

func (w *conn) dialOnce(ctx context.Context) error {
	if w.conn != nil {
		return nil
	}

	dialer := &net.Dialer{Timeout: w.cfg.Timeout}
	var nc net.Conn
	var err error

	if w.cfg.TLSConfig != nil {
		nc, err = tls.DialWithDialer(dialer, "tcp", w.cfg.Address, w.cfg.TLSConfig)
	} else {
		nc, err = dialer.DialContext(ctx, "tcp", w.cfg.Address)
	}
	if err != nil {
		return err
	}

	opts := drpcconn.Options{
		Manager: drpcmanager.Options{
			Reader: drpcwire.ReaderOptions{
				MaximumBufferSize: w.cfg.MaxBufferBytes,
			},
		},
	}

	w.conn = drpcconn.NewWithOptions(nc, opts)
	w.client = proto.NewDRPCMMDBClient(w.conn)
	return nil
}

func (w *conn) ensureClient(ctx context.Context) (proto.DRPCMMDBClient, error) {
	w.mu.RLock()
	c := w.client
	w.mu.RUnlock()
	if c != nil {
		return c, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.client != nil {
		return w.client, nil
	}

	err := w.dialOnce(ctx)
	return w.client, err
}

func (w *conn) markBroken() {
	w.mu.Lock()
	if w.conn != nil {
		_ = w.conn.Close()
	}
	w.conn = nil
	w.client = nil
	w.mu.Unlock()
}

type Client struct {
	cfg  Config
	pool []*conn
	rr   uint32
}

func Dial(ctx context.Context, cfg Config) (*Client, error) {
	applyDefaults(&cfg)

	pool := make([]*conn, cfg.PoolSize)
	for i := 0; i < cfg.PoolSize; i++ {
		w := &conn{cfg: &cfg}
		_ = w.dialOnce(ctx)
		pool[i] = w
	}

	return &Client{cfg: cfg, pool: pool}, nil
}

func applyDefaults(c *Config) {
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if c.PoolSize <= 0 {
		c.PoolSize = 8
	}
	if c.ReconnectInterval == 0 {
		c.ReconnectInterval = 2 * time.Second
	}
	if c.MaxRetriesPerCall <= 0 {
		c.MaxRetriesPerCall = c.PoolSize
	}
	if c.MaxBufferBytes == 0 {
		c.MaxBufferBytes = 512 << 20 // 512 MiB
	}
}

func (c *Client) Close() error {
	var firstErr error
	for _, w := range c.pool {
		w.mu.Lock()
		if w.conn != nil {
			if err := w.conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			w.conn = nil
			w.client = nil
		}
		w.mu.Unlock()
	}
	return firstErr
}

func (c *Client) pickConn() *conn {
	idx := atomic.AddUint32(&c.rr, 1)
	return c.pool[int(idx)%len(c.pool)]
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if drpc.ClosedError.Has(err) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	errStr := err.Error()
	connectionErrors := []string{
		"manager closed",
		"connection refused",
		"broken pipe",
		"connection reset",
		"network is unreachable",
		"no route to host",
		"connection timed out",
		"transport closed",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errStr), connErr) {
			return true
		}
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

func call[T any](c *Client, ctx context.Context, fn func(proto.DRPCMMDBClient) (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt < c.cfg.MaxRetriesPerCall; attempt++ {
		w := c.pickConn()

		cli, err := w.ensureClient(ctx)
		if err != nil {
			lastErr = err
			if attempt < c.cfg.MaxRetriesPerCall-1 {
				time.Sleep(c.cfg.ReconnectInterval)
			}
			continue
		}

		res, err := fn(cli)
		if err == nil {
			return res, nil
		}

		if isConnectionError(err) {
			w.markBroken()
			lastErr = err
			if attempt < c.cfg.MaxRetriesPerCall-1 {
				time.Sleep(c.cfg.ReconnectInterval)
			}
			continue
		}

		return zero, err
	}

	return zero, lastErr
}

func (c *Client) CreateTable(ctx context.Context, name string, comp proto.CompressionMethod, ifNotExists bool) (*proto.Table, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.Table, error) {
		resp, err := cli.CreateTable(ctx, &proto.CreateTableRequest{
			Name:        name,
			Compression: comp,
			IfNotExists: ifNotExists,
		})
		if err != nil {
			return nil, err
		}
		return resp.Table, nil
	})
}

func (c *Client) DropTable(ctx context.Context, name string) error {
	_, err := call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.DropTableResponse, error) {
		return cli.DropTable(ctx, &proto.DropTableRequest{Name: name})
	})
	return err
}

func (c *Client) Insert(ctx context.Context, req *proto.InsertRequest) (*proto.InsertResponse, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.InsertResponse, error) {
		return cli.Insert(ctx, req)
	})
}

func (c *Client) Query(ctx context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.QueryResponse, error) {
		return cli.Query(ctx, req)
	})
}

type StreamQueryParams struct {
	req     *proto.QueryRequest
	onRow   func(*proto.Row) error
	onBatch func([]*proto.Row) error
}

func NewStreamQueryParams() *StreamQueryParams {
	return &StreamQueryParams{
		req:     nil,
		onRow:   func(r *proto.Row) error { return nil },
		onBatch: func(rows []*proto.Row) error { return nil },
	}
}

func (p *StreamQueryParams) WithOnRow(onRow func(*proto.Row) error) *StreamQueryParams {
	p.onRow = onRow
	return p
}

func (p *StreamQueryParams) WithOnBatch(onBatch func([]*proto.Row) error) *StreamQueryParams {
	p.onBatch = onBatch
	return p
}

func (p *StreamQueryParams) WithRequest(req *proto.QueryRequest) *StreamQueryParams {
	p.req = req
	return p
}

func (c *Client) StreamQuery(ctx context.Context, params *StreamQueryParams) (*proto.QueryResponse, error) {
	if params.req == nil {
		return nil, errors.New("request is required")
	}

	type streamResult struct {
		stream proto.DRPCMMDB_StreamQueryClient
		conn   *conn
	}

	var result streamResult
	var lastErr error

	for attempt := 0; attempt < c.cfg.MaxRetriesPerCall; attempt++ {
		w := c.pickConn()

		cli, err := w.ensureClient(ctx)
		if err != nil {
			lastErr = err
			if attempt < c.cfg.MaxRetriesPerCall-1 {
				time.Sleep(c.cfg.ReconnectInterval)
			}
			continue
		}

		stream, err := cli.StreamQuery(ctx, params.req)
		if err == nil {
			result.stream = stream
			result.conn = w
			break
		}

		if isConnectionError(err) {
			w.markBroken()
			lastErr = err
			if attempt < c.cfg.MaxRetriesPerCall-1 {
				time.Sleep(c.cfg.ReconnectInterval)
			}
			continue
		}

		return nil, err
	}

	if result.stream == nil {
		return nil, lastErr
	}

	var (
		decompressRows bool
		decomp         crypto.Compressor
	)
	if params.req.SendCompressed {
		compHelper, _ := crypto.NewCompressor()
		decomp = compHelper.Get(int32(proto.CompressionMethod_CompressionNone))
	}

	resp := &proto.QueryResponse{}

	for {
		chunk, recvErr := result.stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				return resp, nil
			}
			if isConnectionError(recvErr) {
				result.conn.markBroken()
			}
			return nil, recvErr
		}

		switch t := chunk.Chunk.(type) {
		case *proto.StreamQueryChunk_Header:
			h := t.Header
			resp.TableName = h.TableName
			resp.Prefix = h.Prefix
			resp.Compression = h.Compression

			if params.req.SendCompressed &&
				h.Compression != proto.CompressionMethod_CompressionNone {

				compHelper, _ := crypto.NewCompressor()
				decomp = compHelper.Get(int32(h.Compression))
				decompressRows = true
			}
		case *proto.StreamQueryChunk_Batch:
			for _, r := range t.Batch.Rows {
				if decompressRows {
					var derr error
					r.Data, derr = decomp.Decompress(r.Data)
					if derr != nil {
						return nil, derr
					}
				}
				if err := params.onRow(r); err != nil {
					return nil, err
				}
			}
			if err := params.onBatch(t.Batch.Rows); err != nil {
				return nil, err
			}
		case *proto.StreamQueryChunk_Footer:
			f := t.Footer
			resp.Duration = f.Duration
			resp.Count = f.Count
			resp.CompressedBytes = f.CompressedBytes
			resp.UncompressedBytes = f.UncompressedBytes
			resp.TruncatedByLimit = f.TruncatedByLimit
		}
	}
}

func (c *Client) GetTable(ctx context.Context, name string) (*proto.Table, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.Table, error) {
		resp, err := cli.GetTable(ctx, &proto.GetTableRequest{TableName: name})
		if err != nil {
			return nil, err
		}
		return resp.Table, nil
	})
}

func (c *Client) ListTables(ctx context.Context) ([]*proto.Table, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) ([]*proto.Table, error) {
		resp, err := cli.ListTables(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		return resp.Tables, nil
	})
}

func (c *Client) Backup(ctx context.Context, version uint64, comp proto.CompressionMethod, handler func(*proto.BackupChunk) error) error {
	stream, err := call(c, ctx, func(cli proto.DRPCMMDBClient) (proto.DRPCMMDB_BackupClient, error) {
		return cli.Backup(ctx, &proto.BackupRequest{Version: version, Compression: comp})
	})
	if err != nil {
		return err
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := handler(chunk); err != nil {
			return err
		}
	}
}

func (c *Client) BackupToS3(ctx context.Context, req *proto.S3BackupRequest) (*proto.S3BackupResponse, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.S3BackupResponse, error) {
		return cli.BackupToS3(ctx, req)
	})
}

func (c *Client) RestoreFromS3(ctx context.Context, req *proto.RestoreFromS3Request) (*proto.RestoreFromS3Response, error) {
	return call(c, ctx, func(cli proto.DRPCMMDBClient) (*proto.RestoreFromS3Response, error) {
		return cli.RestoreFromS3(ctx, req)
	})
}
