package api

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"esAdb/common"
	"esAdb/store"
)

//go:embed web/*
var webFS embed.FS

// MonitorAPI 监控 SSE 与可视化页面
type MonitorAPI struct {
	mgr *store.Manager
}

func NewMonitorAPI(mgr *store.Manager) *MonitorAPI {
	return &MonitorAPI{mgr: mgr}
}

// HandleIndex 监控首页
func (a *MonitorAPI) HandleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := fs.ReadFile(webFS, "web/index.html")
	if err != nil {
		http.Error(w, "页面不存在", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// HandleSSE GET /monitor/sse  推送增量/补全监控（保留 1 小时）
func (a *MonitorAPI) HandleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 GET")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		common.WriteFail(w, http.StatusInternalServerError, 500, "不支持 SSE")
		return
	}
	if a.mgr == nil || a.mgr.Monitor == nil {
		common.WriteFail(w, http.StatusServiceUnavailable, 503, "监控未就绪")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	send := func(event string, data interface{}) error {
		b, err := common.MarshalJSON(data, false)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send("history", a.mgr.Monitor.History()); err != nil {
		return
	}
	if err := send("pipeline", a.mgr.Monitor.Pipeline()); err != nil {
		return
	}
	if err := send("runtime", store.RuntimeStatsNow()); err != nil {
		return
	}

	subID, subCh := a.mgr.Monitor.Subscribe()
	defer a.mgr.Monitor.Unsubscribe(subID)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-subCh:
			if !ok {
				return
			}
			if err := send(msg.Event, msg.Data); err != nil {
				return
			}
		case <-ticker.C:
			if err := send("pipeline", a.mgr.Monitor.Pipeline()); err != nil {
				return
			}
			if err := send("runtime", store.RuntimeStatsNow()); err != nil {
				return
			}
		}
	}
}

// HandleBackfillSSE GET /monitor/backfill/sse  补全详情（打开弹框时连接，1s 推送）
func (a *MonitorAPI) HandleBackfillSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 GET")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		common.WriteFail(w, http.StatusInternalServerError, 500, "不支持 SSE")
		return
	}
	if a.mgr == nil || a.mgr.Monitor == nil {
		common.WriteFail(w, http.StatusServiceUnavailable, 503, "监控未就绪")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	send := func(event string, data interface{}) error {
		b, err := common.MarshalJSON(data, false)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	if err := send("snapshot", a.mgr.Monitor.BuildBackfillDetail()); err != nil {
		return
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := send("detail", a.mgr.Monitor.BuildBackfillDetail()); err != nil {
				return
			}
		}
	}
}
