package ssdb

import (
	"bytes"
	"strconv"
)

var (
	_REPLY_OK           = []byte("ok")
	_REPLY_NOT_FOUND    = []byte("not_found")
	_REPLY_ERROR        = []byte("error")
	_REPLY_FAIL         = []byte("fail")
	_REPLY_CLIENT_ERROR = []byte("client_error")
)

type State int

func (state State) String() string {
	switch state {
	case _OK:
		return string(_REPLY_OK)
	case _NOT_FOUND:
		return string(_REPLY_NOT_FOUND)
	case _ERROR:
		return string(_REPLY_ERROR)
	case _FAIL:
		return string(_REPLY_FAIL)
	case _CLIENT_ERROR:
		return string(_REPLY_CLIENT_ERROR)
	}
	return "undefined"
}

func (state State) IsOk() bool {
	return state == _OK
}

func (state State) IsNotFound() bool {
	return state == _NOT_FOUND
}

func (state State) IsError() bool {
	return state == _ERROR
}

func (state State) IsFail() bool {
	return state == _FAIL
}

func (state State) IsClientError() bool {
	return state == _CLIENT_ERROR
}

const (
	_UNDEFINED = iota
	_OK
	_NOT_FOUND
	_ERROR
	_FAIL
	_CLIENT_ERROR
)

type Reply struct {
	State State
	data  [][]byte
}

func (r *Reply) toState(line []byte) {
	if bytes.Equal(line, _REPLY_OK) {
		r.State = _OK
	} else if bytes.Equal(line, _REPLY_NOT_FOUND) {
		r.State = _NOT_FOUND
	} else if bytes.Equal(line, _REPLY_ERROR) {
		r.State = _ERROR
	} else if bytes.Equal(line, _REPLY_FAIL) {
		r.State = _FAIL
	} else if bytes.Equal(line, _REPLY_CLIENT_ERROR) {
		r.State = _CLIENT_ERROR
	}
}

func (r *Reply) GetData() [][]byte {
	return r.data
}

func (r *Reply) String() string {
	if len(r.data) == 0 {
		return ""
	}
	return string(r.data[0])
}

func (r *Reply) Int() int {
	return int(r.Int64())
}

func (r *Reply) Bytes() []byte {
	if len(r.data) > 0 {
		return r.data[0]
	}
	return []byte{}
}

func (r *Reply) Int64() int64 {
	if len(r.data) == 0 {
		return 0
	}
	i64, err := strconv.ParseInt(string(r.data[0]), 10, 64)
	if err == nil {
		return i64
	}
	return 0
}

func (r *Reply) Float64() float64 {
	if len(r.data) == 0 {
		return 0
	}
	f64, err := strconv.ParseFloat(string(r.data[0]), 64)
	if err == nil {
		return f64
	}
	return 0
}

func (r *Reply) Bool() bool {
	if len(r.data) == 0 {
		return false
	}
	b, err := strconv.ParseBool(string(r.data[0]))
	if err == nil {
		return b
	}
	return false
}

func (r *Reply) Strings() []string {
	size := len(r.data)
	if size > 0 {
		ret := make([]string, size)
		for i := 0; i < size; i++ {
			ret[i] = string(r.data[i])
		}
		return ret
	}
	return []string{}
}

func (r *Reply) KeyVals() map[string]string {
	size := len(r.data)
	if size > 0 {
		ret := make(map[string]string, size)
		for i := 0; i < size; i += 2 {
			ret[string(r.data[i])] = string(r.data[i+1])

		}
		return ret
	}
	return map[string]string{}
}
