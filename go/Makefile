# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	EXT := dylib
else ifeq ($(OS),Windows_NT)
	EXT := dll
else
	EXT := so
endif

VERSION ?= v0.1.0
LIB := build/libadbc_driver_redis.$(EXT)
CLI := build/redis-arrow
LDFLAGS := -s -w -X github.com/adbc-drivers/driverbase-go/driverbase.infoDriverVersion=$(VERSION)

.PHONY: build cli test validate clean redis-up redis-down

build: $(LIB)

$(LIB): $(wildcard *.go) $(wildcard pkg/*.go) $(wildcard pkg/*.c) $(wildcard pkg/*.h) go.mod go.sum
	mkdir -p build
	CGO_ENABLED=1 go build -buildmode=c-shared -tags driverlib -ldflags "$(LDFLAGS)" -o $@ ./pkg

# redis-arrow: Arrow IPC export and import. It links the driver as a Go
# library, so it needs no cgo.
cli: $(CLI)

$(CLI): $(wildcard *.go) $(wildcard cmd/redis-arrow/*.go) go.mod go.sum
	mkdir -p build
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o $@ ./cmd/redis-arrow

# One package at a time: with REDIS_URI set, the driver's and redis-arrow's
# integration tests share the server, and any connection that opens rolls
# back an abandoned move (adbc.redis.rename_rekey), including the ones the
# driver's tests set up and expect to find.
test:
	go test -v -p 1 ./...

redis-up:
	docker compose up --detach --wait redis

redis-down:
	docker compose down

validate: build
	cd validation && uv run pytest -vvs -rfEsxX tests/

clean:
	rm -rf build
