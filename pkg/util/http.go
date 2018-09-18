package util

import (
	"fmt"

	"github.com/ngaut/log"
)

const (
	statusOK = 200
	// nolint
	statusWrongParameter = 1
	statusNotFound       = 2
	statusOtherError     = 3
)

// Response represents message that returns to client
type Response struct {
	Message string      `json:"message"`
	Code    int         `json:"code"`
	Data    interface{} `json:"data"`
}

// SuccessResponse returns a success response.
func SuccessResponse(message string, data interface{}) *Response {
	return &Response{
		Code:    statusOK,
		Data:    data,
		Message: message,
	}
}

// NotFoundResponsef returns a not found response.
func NotFoundResponsef(format string, args ...interface{}) *Response {
	format = format + " not found"
	return &Response{
		Code:    statusNotFound,
		Message: fmt.Sprintf(format, args...),
	}
}

// ErrResponsef returns a error response.
func ErrResponsef(format string, args ...interface{}) *Response {
	errMsg := fmt.Sprintf(format, args...)
	log.Warnf(errMsg)
	return &Response{
		Code:    statusOtherError,
		Message: errMsg,
	}
}
