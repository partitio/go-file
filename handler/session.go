package handler

import (
	"sync"

	"github.com/spf13/afero"
)

type session struct {
	sync.Mutex
	files   map[int64]afero.File
	counter int64
}

func (s *session) Add(file afero.File) int64 {
	s.Lock()
	defer s.Unlock()

	s.counter += 1
	s.files[s.counter] = file

	return s.counter
}

func (s *session) Get(id int64) afero.File {
	s.Lock()
	defer s.Unlock()
	return s.files[id]
}

func (s *session) Delete(id int64) {
	s.Lock()
	defer s.Unlock()

	if file, exist := s.files[id]; exist {
		file.Close()
		delete(s.files, id)
	}
}

func (s *session) Len() int {
	return len(s.files)
}
