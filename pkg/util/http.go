// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"

	"github.com/pingcap/log"
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
	log.Warn(errMsg)
	return &Response{
		Code:    statusOtherError,
		Message: errMsg,
	}
}
