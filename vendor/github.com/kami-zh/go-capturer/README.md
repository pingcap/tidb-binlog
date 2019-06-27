# go-capturer

[![GoDoc](https://godoc.org/github.com/kami-zh/go-capturer?status.svg)](https://godoc.org/github.com/kami-zh/go-capturer)
[![Build Status](https://travis-ci.org/kami-zh/go-capturer.svg?branch=master)](https://travis-ci.org/kami-zh/go-capturer)
[![Go Report Card](https://goreportcard.com/badge/github.com/kami-zh/go-capturer)](https://goreportcard.com/report/github.com/kami-zh/go-capturer)

Capture `os.Stdout` and/or `os.Stderr` in Go.
This package is useful for writing tests which print some outputs using `fmt` package.

## Usage

This package provides `CaptureStdout()`, `CaptureStderr()` and `CaptureOutput()` functions to capture outputs.

```go
package main

import (
	"fmt"
	"os"

	"github.com/kami-zh/go-capturer"
)

func ExampleCaptureStdout() {
	out := capturer.CaptureStdout(func() {
		fmt.Fprint(os.Stdout, "foo")
	})

	fmt.Println(out)
	// Output: foo
}

func ExampleCaptureStderr() {
	out := capturer.CaptureStderr(func() {
		fmt.Fprint(os.Stderr, "bar")
	})

	fmt.Println(out)
	// Output: bar
}

func ExampleCaptureOutput() {
	out := capturer.CaptureOutput(func() {
		fmt.Fprint(os.Stdout, "foo")
		fmt.Fprint(os.Stderr, "bar")
	})

	fmt.Println(out)
	// Output: foobar
}
```

## Installation

```
$ go get github.com/kami-zh/go-capturer
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/kami-zh/go-capturer.

## License

The package is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
