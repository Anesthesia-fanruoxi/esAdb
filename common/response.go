package common

import (
	"bytes"
	"encoding/json"
	"net/http"
)

// Resp 统一响应体
type Resp struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// ReqOK 成功
func OK(data interface{}) Resp {
	return Resp{Code: 0, Message: "ok", Data: data}
}

// Fail 失败
func Fail(code int, message string) Resp {
	if code == 0 {
		code = -1
	}
	return Resp{Code: code, Message: message}
}

// MarshalJSON 不转义 & < >（避免 & 变成 \u0026）
func MarshalJSON(v interface{}, indent bool) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if indent {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	return b, nil
}

// WriteJSON 写出统一 JSON 响应
func WriteJSON(w http.ResponseWriter, httpStatus int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)
	b, err := MarshalJSON(v, false)
	if err != nil {
		return
	}
	_, _ = w.Write(b)
}

// WriteOK / WriteFail 快捷方法
func WriteOK(w http.ResponseWriter, data interface{}) {
	WriteJSON(w, http.StatusOK, OK(data))
}

func WriteFail(w http.ResponseWriter, httpStatus int, code int, message string) {
	WriteJSON(w, httpStatus, Fail(code, message))
}
