package ssdb

import (
	"bytes"
	"strconv"
)

var (
	replyOk          = []byte("ok")
	replyNotFound    = []byte("not_found")
	replyError       = []byte("error")
	replyFail        = []byte("fail")
	replyClientError = []byte("client_error")
	replyUndefined   = []byte("undefined")
)

type State int

func (state State) String() string {
	switch state {
	case okType:
		return string(replyOk)
	case notFoundType:
		return string(replyNotFound)
	case errorType:
		return string(replyError)
	case failType:
		return string(replyFail)
	case clientErrorType:
		return string(replyClientError)
	case undefinedType:
		return string(replyUndefined)
	}
	return string(replyUndefined)
}

func (state State) IsOk() bool {
	return state == okType
}

func (state State) IsNotFound() bool {
	return state == notFoundType
}

func (state State) IsError() bool {
	return state == errorType
}

func (state State) IsFail() bool {
	return state == failType
}

func (state State) IsClientError() bool {
	return state == clientErrorType
}

const (
	undefinedType = iota
	okType
	notFoundType
	errorType
	failType
	clientErrorType
)

type Reply struct {
	State State
	data  [][]byte
}

func (r *Reply) toState(line []byte) {
	if bytes.Equal(line, replyOk) {
		r.State = okType
	} else if bytes.Equal(line, replyNotFound) {
		r.State = notFoundType
	} else if bytes.Equal(line, replyError) {
		r.State = errorType
	} else if bytes.Equal(line, replyFail) {
		r.State = failType
	} else if bytes.Equal(line, replyClientError) {
		r.State = clientErrorType
	} else {
		r.State = undefinedType
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

func (r *Reply) BytesArray() [][]byte {
	size := len(r.data)
	if size > 0 {
		ret := make([][]byte, size)
		for i := 0; i < size; i++ {
			ret[i] = make([]byte, len(r.data[i]))
			ret[i] = r.data[i]
		}
		return ret
	}
	return [][]byte{}
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
