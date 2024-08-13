package endpoint

import (
	"bufio"
	"csvfiles/internal/filer"
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"strconv"
)

var routingMap = map[string]route{
	"/api/v1/ids": {handler: func(ctx *fasthttp.RequestCtx, h *HttpHandler) {
		if string(ctx.Method()) == fasthttp.MethodPut {
			h.WriteInFile(ctx)
		} else if string(ctx.Method()) == fasthttp.MethodDelete {
			h.DeleteDataFromFile(ctx)
		} else if string(ctx.Method()) == fasthttp.MethodGet {
			h.GetDataFromFile(ctx)
		} else {
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		}
	}},

	"/api/v1/file": {handler: func(ctx *fasthttp.RequestCtx, h *HttpHandler) {
		if string(ctx.Method()) == fasthttp.MethodDelete {
			h.DeleteFile(ctx)
		} else {
			ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		}
	}},

	"/metrics": {handler: func(ctx *fasthttp.RequestCtx, h *HttpHandler) {
		h.metricsHandler(ctx)
	}},
}

type route struct {
	path    string
	handler func(ctx *fasthttp.RequestCtx, h *HttpHandler)
}

type HttpHandler struct {
	metricsHandler fasthttp.RequestHandler
	filerService   *filer.Filer
}

func init() {
	for path, r := range routingMap {
		r.path = path
		routingMap[path] = r
	}
}

func NewHttpHandler(filerService *filer.Filer) *HttpHandler {
	return &HttpHandler{
		filerService:   filerService,
		metricsHandler: fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler()),
	}
}

func (h *HttpHandler) Handle(ctx *fasthttp.RequestCtx) {
	defer func() {
		err := recover()
		if err != nil {
			logrus.Fatalf("fatal error occured during handling, error: %v", err)
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
	Ids       []int  `json:"ids,required"`
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

	if len(file.Ids) == 0 {
		ctx.SetBody([]byte("empty ids"))
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
	}

	err = h.filerService.WriteData(file.Name, file.Ids, file.NewFile, file.NotUnique)
	if err != nil {
		if errors.Is(filer.ErrNewFileIsNotSet, err) {
			ctx.SetBody([]byte(filer.ErrNewFileIsNotSet.Error()))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		if errors.Is(filer.ErrMustBeUnique, err) {
			ctx.SetBody([]byte(filer.ErrMustBeUnique.Error()))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}

func (h *HttpHandler) GetDataFromFile(ctx *fasthttp.RequestCtx) {
	data, err := h.filerService.GetData(string(ctx.QueryArgs().Peek("file")))
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
		for i, id := range data {
			if err = w.WriteByte('\n'); err != nil {
				return
			}
			if _, err = w.WriteString("Num: "); err != nil {
				return
			}
			if _, err = w.WriteString(strconv.Itoa(i)); err != nil {
				return
			}
			if _, err = w.WriteString(" id: "); err != nil {
				return
			}
			if _, err = w.WriteString(strconv.Itoa(id)); err != nil {
				return
			}
		}
	})
}

func (h *HttpHandler) DeleteDataFromFile(ctx *fasthttp.RequestCtx) {
	var f File
	err := json.Unmarshal(ctx.PostBody(), &f)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	err = h.filerService.DeleteData(f.Name, f.Ids)
	if err != nil {
		if errors.Is(filer.ErrFileIsNotExist, err) {
			ctx.SetBody([]byte(filer.ErrFileIsNotExist.Error()))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}

func (h *HttpHandler) DeleteFile(ctx *fasthttp.RequestCtx) {
	err := h.filerService.DeleteFile(string(ctx.QueryArgs().Peek("file")))
	if err != nil {
		if errors.Is(err, filer.ErrFileIsNotExist) {
			ctx.SetBody([]byte(filer.ErrFileIsNotExist.Error()))
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}
