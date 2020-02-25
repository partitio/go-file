package file

import (
	"net/http"

	mclient "github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"
	"github.com/spf13/afero"

	"github.com/partitio/go-file/client"
	"github.com/partitio/go-file/handler"
	"github.com/partitio/go-file/http_handler"
)

func RegisterFileHandler(server server.Server, dir string, fs afero.Fs) error {
	return handler.RegisterHandler(server, dir, fs)
}

func NewClient(service string, c mclient.Client, fs afero.Fs) client.FileClient {
	return client.NewClient(service, c, fs)
}

func NewHttpHandler(service string, c mclient.Client, fs afero.Fs) http.Handler {
	return http_handler.NewFileHandler(client.NewClient(service, c, fs))
}
