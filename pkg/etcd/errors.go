package etcd

import "fmt"

const (
	ErrCodeKeyNotFound int = iota + 1
	ErrCodeKeyExists
)

var errCodeToMessage = map[int] string {
	ErrCodeKeyNotFound:	"key not found",
	ErrCodeKeyExists:	"key exists",
}

func NewKeyNotFoundError(key string) *Error {
	return &Error{
		Code:            ErrCodeKeyNotFound,
		Key:             key,
	}
}

func NewKeyExistsError(key string) *Error {
	return &Error{
		Code:            ErrCodeKeyExists,
		Key:             key,
	}
}

type Error struct {
	Code			int
	Key			string
	AdditionalErrorMsg	string
}

func (r *Error) Error() string {
	return fmt.Sprintf("RegistryError: %s, Code : %d, Key: %s, AddtionalErrorMsg: %s",
		errCodeToMessage[r.Code], r.Code, r.Key, r.AdditionalErrorMsg)
}

func IsNotFound(err error) bool {
	return isErrCode(err, ErrCodeKeyNotFound)
}

func IsNotExist(err error) bool {
	return isErrCode(err, ErrCodeKeyExists)
}

func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}

	if r, ok := err.(*Error); ok {
		return r.Code == code
	}

	return false
}
