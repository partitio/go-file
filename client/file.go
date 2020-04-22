package client

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

var (
	ErrFileClosed = errors.New("File is closed")
)

type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt

	Readdir(count int) ([]os.FileInfo, error)
	Readdirnames(n int) ([]string, error)
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	WriteString(s string) (ret int, err error)

	os.FileInfo

	WithContext(ctx context.Context) File
}

type file struct {
	name         string
	session      int64
	offset       int64
	size         int64
	lastModified time.Time
	c            FileClient
	closed       bool
	mu           sync.RWMutex
	ctx          context.Context
}

func (f *file) Read(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed == true {
		return 0, ErrFileClosed
	}
	b, err := f.c.ReadAt(f.session, f.offset, int64(len(p)))
	if err != nil {
		return 0, err
	}
	copy(p, b)
	f.offset += int64(len(b))
	return len(b), nil
}

func (f *file) Write(p []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed == true {
		return 0, ErrFileClosed
	}
	b, err := f.c.WriteAt(f.session, f.offset, p)
	if err != nil {
		return 0, err
	}
	f.offset += int64(b)
	return b, nil
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed == true {
		return 0, ErrFileClosed
	}
	if f.closed == true {
		return 0, ErrFileClosed
	}
	switch whence {
	case 0:
		f.offset = offset
	case 1:
		f.offset += offset
	case 2:
		f.offset = f.size + offset
	}
	return f.offset, nil
}

func (f *file) ReadAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offset = off
	return f.Read(p)
}

func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.offset = off
	return f.Write(p)
}

func (f *file) Name() string {
	panic("implement me")
}

func (f *file) Readdir(_ int) ([]os.FileInfo, error) {
	return nil, &os.PathError{Op: "readdir", Path: f.name, Err: errors.New("not a dir")}
}

func (f *file) Readdirnames(n int) ([]string, error) {
	return nil, &os.PathError{Op: "readdir", Path: f.name, Err: errors.New("not a dir")}
}

func (f *file) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *file) Sync() error {
	return nil
}

func (f *file) Truncate(size int64) error {
	return errors.New("operation not supported")
}

func (f *file) WriteString(s string) (ret int, err error) {
	return f.Write([]byte(s))
}

func (f *file) Size() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.size
}

func (f *file) Mode() os.FileMode {
	return os.ModePerm
}

func (f *file) ModTime() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastModified
}

func (f *file) IsDir() bool {
	return false
}

func (f *file) Sys() interface{} {
	return nil
}

func (f *file) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.c.Close(f.session)
}

func (f *file) WithContext(ctx context.Context) File {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ctx == nil {
		ctx = context.TODO()
	}
	return &file{
		name:         f.name,
		session:      f.size,
		offset:       f.offset,
		size:         f.size,
		lastModified: f.lastModified,
		c:            f.c,
		closed:       f.closed,
		ctx:          ctx,
	}
}
