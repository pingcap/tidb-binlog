### Makefile for tidb-binlog
.PHONY: build test check update clean pump cistern drainer fmt

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO        := GO15VENDOREXPERIMENT="1" go
GOBUILD   := GOPATH=$(CURDIR)/_vendor:$(GOPATH) $(GO) build
GOTEST    := GOPATH=$(CURDIR)/_vendor:$(GOPATH) $(GO) test

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

dev: check test build

build: pump cistern drainer

pump:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go

cistern:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cistern cmd/cistern/main.go

drainer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/drainer cmd/drainer/main.go

install:
	go install ./...

test:
	@export log_level=error;\
	$(GOTEST) -cover $(PACKAGES)

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
	glide update -s -v -u
endif
	@echo "removing test files"
	glide vc --use-lock-file --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/src

clean:
	go clean -i ./...
	rm -rf *.out


