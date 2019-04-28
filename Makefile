### Makefile for tidb-binlog
.PHONY: build test check update clean pump drainer fmt reparo integration_test arbiter binlogctl

PROJECT=tidb-binlog

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif
FAIL_ON_STDOUT := awk '{ print  } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

TEST_DIR := /tmp/tidb_binlog_test

GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST   := CGO_ENABLED=1 $(GO) test -p 3

ARCH  := "`uname -s`"
LINUX := "Linux"
MAC   := "Darwin"
PACKAGE_LIST := go list ./...| grep -vE 'vendor|proto'
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor' | grep -vE 'binlog.pb.go')

LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.ReleaseVersion=$(shell git describe --tags --dirty)"

default: build buildsucc

buildsucc:
	@echo Build TiDB Binlog Utils successfully!

all: dev install

dev: check test

build: pump drainer reparo arbiter binlogctl

pump:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go

drainer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/drainer cmd/drainer/main.go

arbiter:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/arbiter cmd/arbiter/main.go

reparo:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/reparo cmd/reparo/main.go

binlogctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/binlogctl cmd/binlogctl/main.go

install:
	go install ./...

test:
	mkdir -p "$(TEST_DIR)"
	@export log_level=error;\
	$(GOTEST) -cover -covermode=count -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES)

integration_test: build
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/sync_diff_inspector
	@which bin/drainer
	@which bin/pump
	@which bin/binlogctl
	@which bin/reparo
	tests/run.sh

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

lint:tools/bin/revive
	@echo "linting"
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml $(FILES)

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

check: fmt lint check-static tidy

coverage:
	GO111MODULE=off go get github.com/wadey/gocovmerge
	gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	@goveralls -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
	grep -F '<option' "$(TEST_DIR)/all_cov.html"
endif

check-static: tools/bin/golangci-lint
	$(GO) mod vendor
	tools/bin/golangci-lint --disable errcheck run $$($(PACKAGE_DIRECTORIES))

clean:
	go clean -i ./...
	rm -rf *.out

tools/bin/revive: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/revive github.com/mgechev/revive

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check; \
	$(GO) build -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint
