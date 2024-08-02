package main

import (
	"csvfiles/internal/endpoint"
	"csvfiles/internal/filer"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"os/signal"
)

var (
	storage     *filer.Storage
	httpHandler *endpoint.HttpHandler
)

func main() {
	mustInitConfig()
	mustInitStorage()
	setupHttpHandler()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)
	<-sigChan
}

func setupHttpHandler() {
	httpHandler = endpoint.NewHttpHandler(storage)
	go func() {
		logrus.Info("Server started")
		err := fasthttp.ListenAndServe(":8999", httpHandler.Handle)
		if err != nil {
			log.Fatal("Listen error: ", err.Error())
		}
	}()
}

func mustInitConfig() {
	viper.SetConfigFile("./configuration.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("failed to read configuration file, error: %v", err)
	}
}

func mustInitStorage() {
	storage = filer.NewStorage()
	if storage.LoadAllData() != nil {
		log.Fatal("failed to get all data")
	}
}
