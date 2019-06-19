FROM golang:alpine as builder

WORKDIR /go/src/github.com/partitio/go-file

COPY . .

RUN go build -v -o /usr/local/bin/file-srv cmd/file-srv/main.go

FROM alpine

COPY --from=builder /usr/local/bin/file-srv /usr/local/bin/file-srv

EXPOSE 18888

ENTRYPOINT ["file-srv"]
