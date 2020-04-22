package client

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/micro/go-micro/client"
	"github.com/spf13/afero"
	"golang.org/x/net/context"

	proto "github.com/partitio/go-file/proto"
)

// FileClient is the client interface to access files
type FileClient interface {
	Open(filename string) (int64, error)

	Read(sessionId int64, buf []byte) (int, error)
	ReadAt(sessionId, offset, size int64) ([]byte, error)
	GetBlock(sessionId, blockId int64) ([]byte, error)

	Download(filename, saveFile string) error
	DownloadAt(filename, saveFile string, blockId int) error

	Create(filename string) (int64, error)

	Write(sessionID int64, buf []byte) (int, error)
	WriteAt(sessionId, offset int64, buf []byte) (int, error)
	SetBlock(sessionId, blockId int64, buf []byte) error

	Upload(filename, saveFile string) error
	UploadAt(filename, saveFile string, blockId int) error

	Stat(filename string) (*proto.StatResponse, error)

	Close(sessionId int64) error
}
type FileClientWithContext interface {
	OpenWithContext(ctx context.Context, filename string) (int64, error)

	ReadWithContext(ctx context.Context, sessionId int64, buf []byte) (int, error)
	ReadAtWithContext(ctx context.Context, sessionId, offset, size int64) ([]byte, error)
	GetBlockWithContext(ctx context.Context, sessionId, blockId int64) ([]byte, error)

	DownloadWithContext(ctx context.Context, filename, saveFile string) error
	DownloadAtWithContext(ctx context.Context, filename, saveFile string, blockId int) error

	CreateWithContext(ctx context.Context, filename string) (int64, error)

	WriteWithContext(ctx context.Context, sessionID int64, buf []byte) (int, error)
	WriteAtWithContext(ctx context.Context, sessionId, offset int64, buf []byte) (int, error)
	SetBlockWithContext(ctx context.Context, sessionId, blockId int64, buf []byte) error

	UploadWithContext(ctx context.Context, filename, saveFile string) error
	UploadAtWithContext(ctx context.Context, filename, saveFile string, blockId int) error

	StatWithContext(ctx context.Context, filename string) (*proto.StatResponse, error)

	CloseWithContext(ctx context.Context, sessionId int64) error
}
const (
	BlockSize = 512 * 1024
)

type fc struct {
	c  proto.FileService
	os *afero.Fs
}

func (c *fc) Open(filename string) (int64, error) {
	return c.OpenWithContext(context.TODO(), filename)
}

func (c *fc) Stat(filename string) (*proto.StatResponse, error) {
	return c.StatWithContext(context.TODO(), filename)
}

func (c *fc) GetBlock(sessionId, blockId int64) ([]byte, error) {
	return c.GetBlockWithContext(context.TODO(), sessionId, blockId)
}

func (c *fc) GetBlockWithContext(ctx context.Context, sessionId, blockId int64) ([]byte, error) {
	return c.ReadAtWithContext(ctx, sessionId, blockId*BlockSize, BlockSize)
}

func (c *fc) ReadAt(sessionId, offset, size int64) ([]byte, error) {
	return c.ReadAtWithContext(context.TODO(), sessionId, offset, size)
}

func (c *fc) Read(sessionId int64, buf []byte) (int, error) {
	return c.ReadWithContext(context.TODO(), sessionId, buf)
}

func (c *fc) Close(sessionId int64) error {
	return c.CloseWithContext(context.TODO(), sessionId)
}

func (c *fc) Download(filename, saveFile string) error {
	return c.DownloadWithContext(context.TODO(), filename, saveFile)
}

func (c *fc) DownloadAt(filename, saveFile string, blockId int) error {
	return c.DownloadAtWithContext(context.TODO(), filename, saveFile, blockId)
}

func (c *fc) SetBlock(sessionId, blockId int64, buf []byte) error {
	return c.SetBlockWithContext(context.TODO(), sessionId, blockId, buf)
}

func (c *fc) Create(filename string) (int64, error) {
	return c.CreateWithContext(context.TODO(), filename)
}

func (c *fc) Upload(filename, saveFile string) error {
	return c.UploadWithContext(context.TODO(), filename, saveFile)
}

func (c *fc) UploadAt(filename, saveFile string, blockId int) error {
	return c.UploadAtWithContext(context.TODO(), filename, saveFile, blockId)
}

func (c *fc) Write(sessionID int64, buf []byte) (int, error) {
	return c.WriteWithContext(context.TODO(), sessionID,  buf)
}

func (c *fc) WriteAt(sessionId, offset int64, buf []byte) (int, error) {
	return c.WriteAtWithContext(context.TODO(), sessionId, offset, buf)
}

// NewClient returns a new FileClient which uses a micro FileClient
func NewClient(service string, c client.Client, fs *afero.Fs) FileClient {
	return &fc{proto.NewFileService(service, c), fs}
}

func (c *fc) WriteWithContext(ctx context.Context, sessionID int64, buf []byte) (int, error) {
	return c.WriteAtWithContext(ctx, sessionID, 0, buf)
}

func (c *fc) WriteAtWithContext(ctx context.Context, sessionId, offset int64, buf []byte) (int, error) {
	rsp, err := c.c.Write(ctx, &proto.WriteRequest{Id: sessionId, Offset: offset, Data: buf})
	if err != nil {
		return 0, err
	}
	return int(rsp.Size), nil
}
func (c *fc) UploadWithContext(ctx context.Context, filename, saveFile string) error {
	return c.UploadAtWithContext(ctx, filename, saveFile, 0)
}

func (c *fc) SetBlockWithContext(ctx context.Context, sessionId, blockId int64, buf []byte) error {
	_, err := c.WriteAtWithContext(ctx, sessionId, blockId*BlockSize, buf)
	return err
}

func (c *fc) CreateWithContext(ctx context.Context, filename string) (int64, error) {
	rsp, err := c.c.Create(ctx, &proto.CreateRequest{Filename: filename})
	if err != nil {
		return 0, err
	}
	return rsp.Id, nil
}
func (c *fc) UploadAtWithContext(ctx context.Context, filename, saveFile string, blockId int) error {
	if c.os == nil {
		return errors.New("UploadAt cannot use a nil fs")
	}
	fs := *c.os
	stat, err := fs.Stat(filename)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		return errors.New(fmt.Sprintf("%s is a directory", filename))
	}
	sessionId, err := c.CreateWithContext(ctx, saveFile)
	if err != nil {
		return err
	}
	defer c.CloseWithContext(ctx, sessionId)
	f, err := fs.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	blocks := int(stat.Size() / BlockSize)
	if stat.Size()%BlockSize != 0 {
		blocks += 1
	}
	for i := blockId; i < blocks; i++ {
		buf := make([]byte, BlockSize)
		n, err := f.ReadAt(buf, int64(i)*BlockSize)
		if err != nil && err != io.EOF {
			return err
		}
		b := make([]byte, n)
		copy(b, buf)
		if err := c.SetBlockWithContext(ctx, sessionId, int64(i), b); err != nil {
			return err
		}
		if i%((blocks-blockId)/100+1) == 0 {
			log.Printf("Uploading %s [%d/%d] blocks", filename, i-blockId+1, blocks-blockId)
		}
		if err == io.EOF {
			break
		}
	}
	log.Printf("Download %s completed", filename)
	return nil
}

func (c *fc) DownloadWithContext(ctx context.Context, filename, saveFile string) error {
	return c.DownloadAtWithContext(ctx, filename, saveFile, 0)
}

func (c *fc) DownloadAtWithContext(ctx context.Context, filename, saveFile string, blockId int) error {
	stat, err := c.StatWithContext(ctx, filename)
	if err != nil {
		return err
	}
	if stat.Type == "Directory" {
		return errors.New(fmt.Sprintf("%s is directory.", filename))
	}

	blocks := int(stat.Size / BlockSize)
	if stat.Size%BlockSize != 0 {
		blocks += 1
	}
	if c.os == nil {
		return errors.New("UploadAt cannot use a nil fs")
	}
	fs := *c.os
	log.Printf("Download %s in %d blocks\n", filename, blocks-blockId)

	file, err := fs.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	sessionId, err := c.OpenWithContext(ctx, filename)
	if err != nil {
		return err
	}

	for i := blockId; i < blocks; i++ {
		buf, rerr := c.GetBlockWithContext(ctx, sessionId, int64(i))
		if rerr != nil && rerr != io.EOF {
			return rerr
		}
		if _, werr := file.WriteAt(buf, int64(i)*BlockSize); werr != nil {
			return werr
		}

		if i%((blocks-blockId)/100+1) == 0 {
			log.Printf("Downloading %s [%d/%d] blocks", filename, i-blockId+1, blocks-blockId)
		}

		if rerr == io.EOF {
			break
		}
	}
	log.Printf("Download %s completed", filename)

	c.CloseWithContext(ctx, sessionId)

	return nil
}

func (c *fc) CloseWithContext(ctx context.Context, sessionId int64) error {
	_, err := c.c.Close(ctx, &proto.CloseRequest{Id: sessionId})
	return err
}

func (c *fc) ReadWithContext(ctx context.Context, sessionId int64, buf []byte) (int, error) {
	b, err := c.ReadAtWithContext(ctx, sessionId, 0, int64(cap(buf)))
	if err != nil {
		return 0, err
	}
	copy(buf, b)
	return len(b), nil
}

func (c *fc) ReadAtWithContext(ctx context.Context, sessionId, offset, size int64) ([]byte, error) {
	rsp, err := c.c.Read(ctx, &proto.ReadRequest{Id: sessionId, Size: size, Offset: offset})
	if err != nil {
		return nil, err
	}

	if rsp.Eof {
		err = io.EOF
	}

	if rsp.Data == nil {
		rsp.Data = make([]byte, size)
	}

	if size != rsp.Size {
		return rsp.Data[:rsp.Size], err
	}

	return rsp.Data, nil
}

func (c *fc) StatWithContext(ctx context.Context, filename string) (*proto.StatResponse, error) {
	return c.c.Stat(ctx, &proto.StatRequest{Filename: filename})
}

func (c *fc) OpenWithContext(ctx context.Context, filename string) (int64, error) {
	rsp, err := c.c.Open(context.TODO(), &proto.OpenRequest{Filename: filename})
	if err != nil {
		return 0, err
	}
	return rsp.Id, nil
}
