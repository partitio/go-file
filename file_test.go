package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/micro/go-micro"
	"github.com/micro/go-micro/registry/memory"
	"golang.org/x/net/context"

	"github.com/partitio/go-file/client"
	"github.com/partitio/go-file/handler"
	proto "github.com/partitio/go-file/proto"
)

func TestFileServer(t *testing.T) {
	// service cancellation context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	)

	td := os.TempDir()
	f := filepath.Join(td, "/test.file")

	// write a file
	err := ioutil.WriteFile(f, []byte(`hello world`), 0666)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f)

	h, err := handler.NewHandler(td)
	if err != nil {
		t.Fatal(err)
	}
	// register file handler
	if err := proto.RegisterFileHandler(s.Server(), h); err != nil {
		t.Fatal(err)
	}

	// start service
	go s.Run()

	// wait for start
	<-wait

	// new file client
	cl := client.NewClient("go.micro.srv.file", s.Client())

	if err := cl.Upload(f, "server_test.file"); err != nil {
		t.Error(err)
		return
	}
	defer os.Remove("server_test.file")

	if err := cl.Download("server_test.file", "client_test.file"); err != nil {
		// no fatal as we need cleanup
		t.Error(err)
		return
	}
	defer os.Remove("client_test.file")

	// got file!
	b, err := ioutil.ReadFile("client_test.file")
	if err != nil {
		t.Error(err)
		return
	}

	if string(b) != "hello world" {
		t.Errorf("got %s, expected 'hello world'", string(b))
		return
	}
}
