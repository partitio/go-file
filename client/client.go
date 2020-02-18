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

const (
	BlockSize = 512 * 1024
)

type fc struct {
	c proto.FileService
	os afero.Fs
}

func (c *fc) Open(filename string) (int64, error) {
	rsp, err := c.c.Open(context.TODO(), &proto.OpenRequest{Filename: filename})
	if err != nil {
		return 0, err
	}
	return rsp.Id, nil
}

func (c *fc) Stat(filename string) (*proto.StatResponse, error) {
	return c.c.Stat(context.TODO(), &proto.StatRequest{Filename: filename})
}

func (c *fc) GetBlock(sessionId, blockId int64) ([]byte, error) {
	return c.ReadAt(sessionId, blockId*BlockSize, BlockSize)
}

func (c *fc) ReadAt(sessionId, offset, size int64) ([]byte, error) {
	rsp, err := c.c.Read(context.TODO(), &proto.ReadRequest{Id: sessionId, Size: size, Offset: offset})
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

func (c *fc) Read(sessionId int64, buf []byte) (int, error) {
	b, err := c.ReadAt(sessionId, 0, int64(cap(buf)))
	if err != nil {
		return 0, err
	}
	copy(buf, b)
	return len(b), nil
}

func (c *fc) Close(sessionId int64) error {
	_, err := c.c.Close(context.TODO(), &proto.CloseRequest{Id: sessionId})
	return err
}

func (c *fc) Download(filename, saveFile string) error {
	return c.DownloadAt(filename, saveFile, 0)
}

func (c *fc) DownloadAt(filename, saveFile string, blockId int) error {
	stat, err := c.Stat(filename)
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

	log.Printf("Download %s in %d blocks\n", filename, blocks-blockId)

	file, err := c.os.OpenFile(saveFile, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	sessionId, err := c.Open(filename)
	if err != nil {
		return err
	}

	for i := blockId; i < blocks; i++ {
		buf, rerr := c.GetBlock(sessionId, int64(i))
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

	c.Close(sessionId)

	return nil
}

func (c *fc) SetBlock(sessionId, blockId int64, buf []byte) error {
	_, err := c.WriteAt(sessionId, blockId*BlockSize, buf)
	return err
}

func (c *fc) Create(filename string) (int64, error) {
	rsp, err := c.c.Create(context.TODO(), &proto.CreateRequest{Filename: filename})
	if err != nil {
		return 0, err
	}
	return rsp.Id, nil
}

func (c *fc) Upload(filename, saveFile string) error {
	return c.UploadAt(filename, saveFile, 0)
}

func (c *fc) UploadAt(filename, saveFile string, blockId int) error {
	stat, err := c.os.Stat(filename)
	if err != nil {
		return err
	}
	if stat.IsDir() {
		return errors.New(fmt.Sprintf("%s is a directory", filename))
	}
	sessionId, err := c.Create(saveFile)
	if err != nil {
		return err
	}
	defer c.Close(sessionId)
	f, err := c.os.Open(filename)
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
		if err := c.SetBlock(sessionId, int64(i), b); err != nil {
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

func (c *fc) Write(sessionID int64, buf []byte) (int, error) {
	return c.WriteAt(sessionID, 0, buf)
}

func (c *fc) WriteAt(sessionId, offset int64, buf []byte) (int, error) {
	rsp, err := c.c.Write(context.TODO(), &proto.WriteRequest{Id: sessionId, Offset: offset, Data: buf})
	if err != nil {
		return 0, err
	}
	return int(rsp.Size), nil
}

// NewClient returns a new FileClient which uses a micro FileClient
func NewClient(service string, c client.Client, fs afero.Fs) FileClient {
	return &fc{proto.NewFileService(service, c), fs}
}

