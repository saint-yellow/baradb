package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func put(c *gin.Context) {
	var req *RequestJSON
	var resp *ResponseJSON

	if err := c.BindJSON(&req); err != nil {
		resp = newResponseJSON(false, err.Error(), nil)
		response(c, http.StatusBadRequest, resp)
		return
	}

	if err := db.Put([]byte(req.Key), []byte(req.Value)); err != nil {
		resp = newResponseJSON(false, err.Error(), nil)
		response(c, http.StatusBadRequest, resp)
		return
	}

	resp = newResponseJSON(true, "ok", nil)
	response(c, http.StatusCreated, resp)
}

func get(c *gin.Context) {
	key := c.Param("key")
	var resp *ResponseJSON

	value, err := db.Get([]byte(key))
	if err != nil {
		resp = newResponseJSON(false, err.Error(), nil)
		response(c, http.StatusBadRequest, resp)
		return
	}

	resp = newResponseJSON(true, "ok", string(value))
	response(c, http.StatusOK, resp)
}

func delete(c *gin.Context) {
	key := c.Param("key")
	var resp *ResponseJSON

	if err := db.Delete([]byte(key)); err != nil {
		resp = newResponseJSON(false, err.Error(), nil)
		response(c, http.StatusBadRequest, resp)
		return
	}

	resp = newResponseJSON(true, "ok", nil)
	response(c, http.StatusOK, resp)
}

func listKeys(c *gin.Context) {
	keys := db.ListKeys()
	data := make([]string, len(keys))
	for i, k := range keys {
		data[i] = string(k)
	}
	resp := newResponseJSON(true, "ok", data)
	response(c, http.StatusOK, resp)
}

func stat(c *gin.Context) {
	stat := db.Stat()
	resp := newResponseJSON(true, "ok", stat)
	response(c, http.StatusOK, resp)
}
