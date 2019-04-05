
.PHONY: proto
proto:
	@protoc -I. --go_out=. --micro_out=. proto/file.proto
	@echo "proto generated"

.PHONY: clean
clean:
	@rm -rf proto/*.go
	@echo "clean done"
