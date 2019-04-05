package client

import (
	"github.com/micro/go-micro/client"
	proto "github.com/partitio/go-file/proto"
)

// Client is the client interface to access files
type Client interface {
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

// NewClient returns a new Client which uses a micro Client
func NewClient(service string, c client.Client) Client {
	return &fc{proto.NewFileService(service, c)}
}
