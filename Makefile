ARCH      := "`uname -s`"
LINUX     := "Linux"
MAC       := "Darwin"
PACKAGES  := $$(go list ./...| grep -vE 'vendor')
FILES     := $$(find . -name '*.go' -type f | grep -vE 'vendor')

LDFLAGS += -X "github.com/pingcap/tidb-binlog/pump.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/pingcap/tidb-binlog/pump.GitSHA=$(shell git rev-parse HEAD)"

default: build

all: dev install

dev: build check test

build: pump server

pump:
	rm -rf vendor && ln -s _vendor/vendor vendor
	GO15VENDOREXPERIMENT=1 go build -ldflags '$(LDFLAGS)' -o bin/pump cmd/pump/main.go
	rm -rf vendor

server:
	rm -rf vendor && ln -s _vendor/vendor vendor
	GO15VENDOREXPERIMENT=1 go build -ldflags '$(LDFLAGS)' -o bin/binlog-server cmd/binlog-server/main.go
	rm -rf vendor

install:
	rm -rf vendor && ln -s _vendor/vendor vendor
	go install ./...
	rm -rf vendor

test:
	rm -rf vendor && ln -s _vendor/vendor vendor
	rm -rf vendor

fmt:
	go fmt ./...
	@goimports -w $(FILES)

check:
	go get github.com/golang/lint/golint
	go tool vet . 2>&1 | grep -vE 'vendor|render.Delims' | awk '{print} END{if(NR>0) {exit 1}}'
	go tool vet --shadow . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	golint ./... 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'
	gofmt -s -l . 2>&1 | grep -vE 'vendor' | awk '{print} END{if(NR>0) {exit 1}}'

update:
	which glide >/dev/null || curl https://glide.sh/get | sh
	which glide-vc || go get -v -u github.com/sgotti/glide-vc
	rm -r vendor && mv _vendor/vendor vendor || true
	rm -rf _vendor
ifdef PKG
	glide --verbose get --strip-vendor --skip-test ${PKG}
else
	glide --verbose update --strip-vendor --skip-test
endif
	@echo "removing test files"
	glide vc --only-code --no-tests
	mkdir -p _vendor
	mv vendor _vendor/vendor

clean:
	find . -type s -exec rm {} \;
	rm -rf vendor && ln -s _vendor/vendor vendor
	go clean ./...
	rm -rf vendor

.PHONY: build test check update clean pump server fmt

