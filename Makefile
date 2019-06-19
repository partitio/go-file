
.PHONY: proto
proto:
	@protoc -I. --go_out=. --micro_out=. proto/file.proto
	@echo "proto generated"

.PHONY: clean
clean:
	@rm -rf proto/*.go
	@echo "clean done"

.PHONY: dep
dep:
	@GO111MODULE=on go mod vendor

.PHONY: docker
docker: dep
	@docker image build -t partitio/go-file .

.PHONY: test
test: dep
	@GO111MODULE=on go test -v -mod=vendor ./...
