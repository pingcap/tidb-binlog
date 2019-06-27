package capturer

import (
	"bytes"
	"io"
	"os"
)

// Capturer has flags whether capture stdout/stderr or not.
type Capturer struct {
	captureStdout bool
	captureStderr bool
}

// CaptureStdout captures stdout.
func CaptureStdout(f func()) string {
	capturer := &Capturer{captureStdout: true}
	return capturer.capture(f)
}

// CaptureStderr captures stderr.
func CaptureStderr(f func()) string {
	capturer := &Capturer{captureStderr: true}
	return capturer.capture(f)
}

// CaptureOutput captures stdout and stderr.
func CaptureOutput(f func()) string {
	capturer := &Capturer{captureStdout: true, captureStderr: true}
	return capturer.capture(f)
}

func (capturer *Capturer) capture(f func()) string {
	r, w, err := os.Pipe()
	if err != nil {
		panic(err)
	}

	if capturer.captureStdout {
		stdout := os.Stdout
		os.Stdout = w
		defer func() {
			os.Stdout = stdout
		}()
	}

	if capturer.captureStderr {
		stderr := os.Stderr
		os.Stderr = w
		defer func() {
			os.Stderr = stderr
		}()
	}

	f()
	w.Close()

	var buf bytes.Buffer
	io.Copy(&buf, r)

	return buf.String()
}
