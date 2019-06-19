package file

import (
	"net/http"

	mclient "github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"

	"github.com/partitio/go-file/client"
	"github.com/partitio/go-file/handler"
	"github.com/partitio/go-file/http_handler"
)

func RegisterFileHandler(server server.Server, dir string) error {
	return handler.RegisterHandler(server, dir)
}

func NewClient(service string, c mclient.Client) client.FileClient {
	return client.NewClient(service, c)
}

func NewHttpHandler(service string, c mclient.Client) http.Handler {
	return http_handler.NewFileHandler(client.NewClient(service, c))
}
