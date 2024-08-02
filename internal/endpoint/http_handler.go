package endpoint

import (
	filesworker "csvfiles/internal/filer"
	"encoding/json"
	"errors"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var routingMap = map[string]route{
	"/api/v1/id": {handler: func(ctx *fasthttp.RequestCtx, h *HttpHandler) {
		if string(ctx.Method()) == fasthttp.MethodPut {
			h.WriteInFile(ctx)
		} else if string(ctx.Method()) == fasthttp.MethodGet {
			h.GetDataFromFile(ctx)
		} else {
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		}
	}},
}

type route struct {
	path    string
	handler func(ctx *fasthttp.RequestCtx, h *HttpHandler)
}

type HttpHandler struct {
	fileStorage *filesworker.Storage
}

func init() {
	for path, r := range routingMap {
		r.path = path
		routingMap[path] = r
	}
}

func NewHttpHandler(fileStorage *filesworker.Storage) *HttpHandler {
	return &HttpHandler{
		fileStorage: fileStorage,
	}
}

func (h *HttpHandler) Handle(ctx *fasthttp.RequestCtx) {
	defer func() {
		err := recover()
		if err != nil {
			log.Fatalf("fatal error occured during handling, error: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		}
	}()

	if r, ok := routingMap[string(ctx.Path())]; ok {
		r.handler(ctx, h)
	} else {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}
}

type File struct {
	Name      string `json:"name,required"`
	Id        []int  `json:"id,required"`
	NewFile   bool   `json:"new-file"`
	NotUnique bool   `json:"not-unique"`
}

func (h *HttpHandler) WriteInFile(ctx *fasthttp.RequestCtx) {
	var file File
	err := json.Unmarshal(ctx.PostBody(), &file)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	err = h.fileStorage.WriteData(file.Name, file.Id, file.NewFile, file.NotUnique)
	if err != nil {
		if errors.Is(filesworker.ErrNewFileIsNotSet, err) {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		if errors.Is(filesworker.ErrMustBeUnique, err) {
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (h *HttpHandler) GetDataFromFile(ctx *fasthttp.RequestCtx) {
	data, err := h.fileStorage.GetData(string(ctx.QueryArgs().Peek("file")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetBody(data)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
