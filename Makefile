### Makefile for tidb-binlog
.PHONY: build test check update clean pump drainer fmt diff reparo

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

GO        := go
GOBUILD   := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST    := CGO_ENABLED=1 $(GO) test -p 3

ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGES  := $$(go list ./...| grep -vE 'vendor|cmd|test|proto|diff')
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor')

LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.ReleaseVersion=$(shell git describe --tags --dirty)"

default: build buildsucc

buildsucc:
	@echo Build TiDB Binlog Utils successfully!

all: dev install

dev: check test build

build: pump drainer

pump:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go

drainer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/drainer cmd/drainer/main.go

diff:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/diff cmd/diff/main.go

reparo:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/reparo cmd/reparo/main.go	

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
	go get golang.org/x/lint/golint
	@echo "vet"
	@ go tool vet $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "vet --shadow"
	@ go tool vet --shadow $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "golint"
	@ golint ./... 2>&1 | grep -vE '\.pb\.go' | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
ifdef PKG
	glide get -s -v --skip-test ${PKG}
else
	glide update -v
endif
	@echo "removing test files"
	glide vc --use-lock-file --only-code --no-tests

clean:
	go clean -i ./...
	rm -rf *.out


