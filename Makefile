### Makefile for tidb-binlog
.PHONY: build test check update clean pump drainer fmt reparo integration_test arbiter

PROJECT=tidb-binlog

# Ensure GOPATH is set before running build process.
ifeq "$(GOPATH)" ""
	$(error Please set the environment variable GOPATH before running `make`)
endif

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(path_to_add):$(PATH)

TEST_DIR := /tmp/tidb_binlog_test

GO		:= go
GOBUILD   := GO111MODULE=on CGO_ENABLED=0 $(GO) build $(BUILD_FLAG)
GOTEST	:= GO111MODULE=on CGO_ENABLED=1 $(GO) test -p 3

ARCH	  := "`uname -s`"
LINUX	 := "Linux"
MAC	   := "Darwin"
PACKAGE_LIST := go list ./...| grep -vE 'vendor|cmd|test|proto|diff'
PACKAGES  := $$($(PACKAGE_LIST))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/pingcap/$(PROJECT)/||'
FILES	 := $$(find . -name '*.go' -type f | grep -vE 'vendor')

LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pkg/version.GitHash=$(shell git rev-parse HEAD)"

default: build buildsucc

buildsucc:
	@echo Build TiDB Binlog Utils successfully!

all: dev install

dev: check test

build: pump drainer reparo

pump:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go

drainer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/drainer cmd/drainer/main.go

arbiter:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/arbiter cmd/arbiter/main.go

reparo:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/reparo cmd/reparo/main.go	

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
	@ golint ./... 2>&1 | grep -vE '\.pb\.go' | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

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


check-static:
	gometalinter --disable-all --deadline 120s \
		--enable megacheck \
		--enable ineffassign \
		$$($(PACKAGE_DIRECTORIES))

update: update_vendor clean_vendor
update_vendor:
	rm -rf vendor/
	GO111MODULE=on go mod verify
	GO111MODULE=on go mod vendor

clean:
	go clean -i ./...
	rm -rf *.out

clean_vendor:
	hack/clean_vendor.sh


