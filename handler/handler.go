package handler

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/server"
	"golang.org/x/net/context"

	proto "github.com/partitio/go-file/proto"
)

// NewHandler is a handler that can be registered with a micro Server
func NewHandler(readDir string) proto.FileHandler {
	return &handler{
		readDir: readDir,
		session: &session{
			files: make(map[int64]*os.File),
		},
	}
}

// RegisterHandler is a convenience method for registering a handler
func RegisterHandler(s server.Server, readDir string) {
	proto.RegisterFileHandler(s, NewHandler(readDir))
}

type handler struct {
	readDir string
	session *session
}

func (h *handler) Open(ctx context.Context, req *proto.OpenRequest, rsp *proto.OpenResponse) error {
	path := filepath.Join(h.readDir, req.Filename)
	file, err := os.Open(path)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}

	rsp.Id = h.session.Add(file)
	rsp.Result = true

	log.Printf("Open %s, sessionId=%d", req.Filename, rsp.Id)

	return nil
}

func (h *handler) Close(ctx context.Context, req *proto.CloseRequest, rsp *proto.CloseResponse) error {
	h.session.Delete(req.Id)
	log.Printf("Close sessionId=%d", req.Id)
	return nil
}

func (h *handler) Stat(ctx context.Context, req *proto.StatRequest, rsp *proto.StatResponse) error {
	path := filepath.Join(h.readDir, req.Filename)
	fi, err := os.Stat(path)
	if os.IsNotExist(err) {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}

	if fi.IsDir() {
		rsp.Type = "Directory"
	} else {
		rsp.Type = "File"
		rsp.Size = fi.Size()
	}

	rsp.LastModified = fi.ModTime().Unix()
	log.Printf("Stat %s, %#v", req.Filename, rsp)

	return nil
}

func (h *handler) Read(ctx context.Context, req *proto.ReadRequest, rsp *proto.ReadResponse) error {
	file := h.session.Get(req.Id)
	if file == nil {
		return errors.InternalServerError("go.micro.srv.file", "You must call open first.")
	}

	rsp.Data = make([]byte, req.Size)
	n, err := file.ReadAt(rsp.Data, req.Offset)
	if err != nil && err != io.EOF {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}

	if err == io.EOF {
		rsp.Eof = true
	}

	rsp.Size = int64(n)
	rsp.Data = rsp.Data[:n]

	log.Printf("Read sessionId=%d, Offset=%d, n=%d", req.Id, req.Offset, rsp.Size)

	return nil
}

func (h *handler) Create(ctx context.Context, req *proto.CreateRequest, rsp *proto.CreateResponse) error {
	path := filepath.Join(h.readDir, req.Filename)
	file, err := os.Create(path)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}

	rsp.Id = h.session.Add(file)
	rsp.Result = true

	log.Printf("Open %s, sessionId=%d", req.Filename, rsp.Id)

	return nil
}

func (h *handler) Write(ctx context.Context, req *proto.WriteRequest, rsp *proto.WriteResponse) error {
	file := h.session.Get(req.Id)
	if file == nil {
		return errors.InternalServerError("go.micro.srv.file", "You must call open first.")
	}

	n, err := file.WriteAt(req.Data, req.Offset)
	if err != nil && err != io.EOF {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}
	log.Printf("Write sessionId=%d, Offset=%d, n=%d", req.Id, req.Offset, n)
	rsp.Size = int64(n)
	return nil
}
