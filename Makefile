### Makefile for tidb-binlog


# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif

CURDIR := $(shell pwd)
export GOPATH := $(CURDIR)/_vendor:$(GOPATH)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGES  := $$(go list ./...| grep -vE 'vendor')
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor')

LDFLAGS += -X "github.com/pingcap/tidb-binlog/pump.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pump.GitSHA=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/cistern.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/cistern.GitSHA=$(shell git rev-parse HEAD)"

default: build buildsucc

buildsucc:
	@echo Build TiDB Binlog Utils successfully!

all: dev install

dev: build check test

build: pump cistern drainer

pump:
	GO15VENDOREXPERIMENT=1 go build -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go

cistern:
	GO15VENDOREXPERIMENT=1 go build -ldflags '$(LDFLAGS)' -o bin/cistern cmd/cistern/main.go

drainer:
	GO15VENDOREXPERIMENT=1 go build -ldflags '$(LDFLAGS)' -o bin/drainer cmd/drainer/main.go

install:
	go install ./...

test:
	@export log_level=error;\
	GO15VENDOREXPERIMENT=1 go test -cover $(PACKAGES)

fmt:
	go fmt ./...
	@goimports -w $(FILES)

check:
	bash gitcookie.sh
	go get github.com/golang/lint/golint
	@echo "vet"
	@ go tool vet $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "vet --shadow"
	@ go tool vet --shadow $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "golint"
	@ golint ./... 2>&1 | grep -vE '\.pb\.go' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -r vendor && mv _vendor/src vendor || true
	rm -rf _vendor
ifdef PKG
	glide get -s -v --skip-test ${PKG}
else
	glide update -s -v --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/src

clean:
	go clean -i ./...
	rm -rf *.out

.PHONY: build test check update clean pump cistern drainer fmt

