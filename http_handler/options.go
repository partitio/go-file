package http_handler

import (
	"context"
	"net/http"
)

type Option func(o *Options)

type HeaderMatcher func(h http.Header) context.Context

type Options struct {
	headersMatcher HeaderMatcher
}

func WithHeaderMatcher(hm HeaderMatcher) Option {
	return func(o *Options) {
		o.headersMatcher = hm
	}
}
