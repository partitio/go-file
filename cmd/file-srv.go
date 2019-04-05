package main

import (
	"context"
	"github.com/partitio/go-file/client"
	"github.com/partitio/go-file/handler"
	"github.com/partitio/go-file/http_handler"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/registry/memory"

	proto "github.com/partitio/go-file/proto"
)

func main() {
	// service cancellation context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := &http.Server{}
	// wait chan
	wait := make(chan bool)
	r := memory.NewRegistry()
	// make service
	s := micro.NewService(
		micro.Name("go.micro.srv.file"),
		micro.Registry(r),
		micro.Context(ctx),
		micro.AfterStart(func() error {
			close(wait)
			return nil
		}),
		micro.AfterStop(func() error {
			srv.Shutdown(ctx)
			return nil
		}),
	)

	td := os.TempDir()
	f := filepath.Join(td, "/test.file")

	// write a file
	err := ioutil.WriteFile(f, []byte(`hello world`), 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f)

	// register file handler
	proto.RegisterFileHandler(s.Server(), handler.NewHandler(td))

	// start service
	go s.Run()

	// wait for start
	<-wait

	// new file client
	c := client.NewClient("go.micro.srv.file", s.Client())
	h := http_handler.NewFileHandler(c)
	m := http.NewServeMux()
	m.Handle("/uploads", h)
	m.Handle("/uploads/", h)
	srv.Handler = m
	srv.Addr = ":8080"

	srv.ListenAndServe()
}
