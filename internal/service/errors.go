package service

import "fmt"

type ErrCode uint8

// Server error codes
const (
	BadArgument ErrCode = iota + 1
	NotFound
	Conflict
)

type Err struct {
	Code ErrCode
	Msg  string
}

func (e *Err) Error() string {
	return fmt.Sprintf("server error %d: %s", e.Code, e.Msg)
}

func Error(code ErrCode, msg string) *Err {
	return &Err{Code: code, Msg: msg}
}

func Errorf(code ErrCode, format string, a ...interface{}) *Err {
	return &Err{Code: code, Msg: fmt.Sprintf(format, a...)}
}
