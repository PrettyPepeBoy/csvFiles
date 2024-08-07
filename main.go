package main

import (
	"os"
	"os/signal"

	"csvfiles/internal/endpoint"
	"csvfiles/internal/filer"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
)

var (
	filerService *filer.Filer
	httpHandler  *endpoint.HttpHandler
)

func init() {
	filer.RegisterFilerMetrics()
}

func main() {
	setupConfig()
	setupFiler()
	setupHttpHandler()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, os.Kill)
	<-sigChan
}

func setupHttpHandler() {
	httpHandler = endpoint.NewHttpHandler(filerService)
	go func() {
		logrus.Info("Server started")
		err := fasthttp.ListenAndServe(":"+viper.GetString("http-server.port"), httpHandler.Handle)
		if err != nil {
			logrus.Fatal("Listen error: ", err.Error())
		}
	}()
}

func setupConfig() {
	viper.SetConfigFile("./configuration.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		logrus.Fatalf("failed to read configuration file, error: %v", err)
	}
}

func setupFiler() {
	var err error
	if filerService, err = filer.NewFiler(); err != nil {
		logrus.Fatal("failed to get all data")
	}
}
