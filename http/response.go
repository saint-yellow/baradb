package main

import "github.com/gin-gonic/gin"

type ResponseJSON struct {
	Success bool   `json:"key,omitempty"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
}

func newResponseJSON(success bool, message string, data any) *ResponseJSON {
	r := &ResponseJSON{
		Success: success,
		Message: message,
		Data:    data,
	}
	return r
}

func response(context *gin.Context, code int, resp *ResponseJSON) {
	context.JSON(code, resp)
}
