package main

import (
	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	router.Static("/", "./shared/dataset/")
	router.Run(":8818")
}
