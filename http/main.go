package main

import (
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/saint-yellow/baradb"
)

var db *baradb.DB

func init() {
	// Initialize the DB engine
	var err error
	opts := baradb.DefaultDBOptions
	opts.Directory = "/tmp/baradb-http"
	db, err = baradb.LaunchDB(opts)
	if err != nil {
		panic(fmt.Sprintf("Failed to launch a DB engine: %v", err))
	}
}

func main() {
	r := gin.Default()

	api := r.Group("/baradb")
	api.GET("/keys", listKeys)
	api.POST("/", put)
	api.DELETE("/:key", delete)
	api.GET("/:key", get)
	api.GET("/stat", stat)

	r.Run()
}
