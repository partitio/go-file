package handler

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/server"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"golang.org/x/net/context"

	proto "github.com/partitio/go-file/proto"
)

// NewHandler is a handler that can be registered with a micro Server
func NewHandler(dir string, fs afero.Fs) (proto.FileHandler, error) {
	logrus.Tracef("Creating File handler on directory : %s", dir)
	if i, err := fs.Stat(dir); err != nil || !i.IsDir(){
		return nil, fmt.Errorf("%s is not a valid directory", dir)
	}
	return &handler{
		dir: dir,
		fs:  fs,
		session: &session{
			files: make(map[int64]afero.File),
		},
	}, nil
}

// RegisterHandler is a convenience method for registering a handler
func RegisterHandler(s server.Server, dir string, fs afero.Fs) error {
	h, err := NewHandler(dir, fs)
	if err != nil {
		return err
	}
	return proto.RegisterFileHandler(s,h)
}

type handler struct {
	dir     string
	session *session
	fs      afero.Fs
}

func (h *handler) Open(ctx context.Context, req *proto.OpenRequest, rsp *proto.OpenResponse) error {
	path := filepath.Join(h.dir, req.Filename)
	file, err := h.fs.Open(path)
	if err != nil {
		errm := strings.Replace(err.Error(), h.dir, "", -1)
		return errors.BadRequest("go.micro.srv.file", errm)
	}

	rsp.Id = h.session.Add(file)
	rsp.Result = true

	logrus.Tracef("Open %s, sessionId=%d", req.Filename, rsp.Id)

	return nil
}

func (h *handler) Close(ctx context.Context, req *proto.CloseRequest, rsp *proto.CloseResponse) error {
	h.session.Delete(req.Id)
	logrus.Tracef("Close sessionId=%d", req.Id)
	return nil
}

func (h *handler) Stat(ctx context.Context, req *proto.StatRequest, rsp *proto.StatResponse) error {
	path := filepath.Join(h.dir, req.Filename)
	fi, err := h.fs.Stat(path)
	if os.IsNotExist(err) {
		errm := strings.Replace(err.Error(), h.dir, "", -1)
		return errors.BadRequest("go.micro.srv.file", errm)
	}

	if fi.IsDir() {
		rsp.Type = "Directory"
	} else {
		rsp.Type = "File"
		rsp.Size = fi.Size()
	}

	rsp.LastModified = fi.ModTime().Unix()
	logrus.Tracef("Stat %s, %#v", req.Filename, rsp.String())

	return nil
}

func (h *handler) Read(ctx context.Context, req *proto.ReadRequest, rsp *proto.ReadResponse) error {
	file := h.session.Get(req.Id)
	if file == nil {
		return errors.BadRequest("go.micro.srv.file", "You must call open first.")
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

	logrus.Tracef("Read sessionId=%d, Offset=%d, n=%d", req.Id, req.Offset, rsp.Size)

	return nil
}

func (h *handler) Create(ctx context.Context, req *proto.CreateRequest, rsp *proto.CreateResponse) error {
	path := filepath.Join(h.dir, req.Filename)
	file, err := h.fs.Create(path)
	if err != nil {
		return errors.InternalServerError("go.micro.srv.file", err.Error())
	}

	rsp.Id = h.session.Add(file)
	rsp.Result = true

	logrus.Tracef("Open %s, sessionId=%d", req.Filename, rsp.Id)

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
	logrus.Tracef("Write sessionId=%d, Offset=%d, n=%d", req.Id, req.Offset, n)
	rsp.Size = int64(n)
	return nil
}
