package http_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/partitio/go-file/client"
)

type Handler interface {
	http.Handler
	Download(w http.ResponseWriter, r *http.Request)
	Upload(w http.ResponseWriter, r *http.Request)
}

type fileHandler struct {
	client client.FileClient
	opts   *Options
}

func (f *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		f.Download(w, r)
	case http.MethodPost:
		f.Upload(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fileHandler) Download(w http.ResponseWriter, r *http.Request) {
	n := filepath.Base(r.RequestURI)
	logrus.Trace("download request: ", n)
	ctx := f.opts.headersMatcher(r.Header)
	file, _, err := f.client.WithContext(ctx).Open(n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	http.ServeContent(w, r, n, time.Now(), file)
}

func (f *fileHandler) Upload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logrus.Trace("Received upload request")
	// Parse our multipart form, 10 << 20 specifies a maximum
	// upload of 10 MB files.
	if err := r.ParseMultipartForm(client.BlockSize); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	// FormFile returns the first file for the given key `myFile`
	// it also returns the FileHeader so we can get the Filename,
	// the Header and the size of the file
	logrus.Trace("Getting file infos")
	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error Retrieving the File")
		fmt.Println(err)
		return
	}
	defer file.Close()
	logrus.Tracef("Uploading File: %+v\n", handler.Filename)
	logrus.Tracef("File Size: %+v\n", handler.Size)
	logrus.Tracef("MIME Header: %+v\n", handler.Header)

	ctx := f.opts.headersMatcher(r.Header)

	id, err := f.client.WithContext(ctx).Create(handler.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.client.WithContext(ctx).Close(id)

	offset := int64(0)
	percent := 0
	for {
		tp := int(offset * 100 / handler.Size)
		if percent != tp {
			percent = tp
			logrus.Tracef("%s (session id: %d) : Upload %d %%", handler.Filename, id, percent)
		}
		b := make([]byte, client.BlockSize)
		n, err := file.ReadAt(b, offset)
		if err == io.EOF {
			break
		}
		offset += int64(n)
		if _, err := f.client.WithContext(ctx).WriteAt(id, offset, b); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// return that we have successfully uploaded our file!
	res, _ := json.Marshal(map[string]string{"response": "Successfully Uploaded File"})
	w.Write(res)
}

func NewFileHandler(client client.FileClient, options ...Option) http.Handler {
	o := &Options{}
	for _, v := range options {
		v(o)
	}
	if o.headersMatcher == nil {
		o.headersMatcher = func(h http.Header) context.Context {
			return context.Background()
		}
	}
	return &fileHandler{client, o}
}
