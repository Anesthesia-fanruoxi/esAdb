package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"esAdb/common"
	"esAdb/config"
	"esAdb/store"
)

// SyncAPI 同步相关接口（历史范围补数）
type SyncAPI struct {
	cfg *config.Config
}

func NewSyncAPI(cfg *config.Config) *SyncAPI {
	return &SyncAPI{cfg: cfg}
}

type rangeReq struct {
	Start string `json:"start"` // 2006-01-02 15:04:05
	End   string `json:"end"`   // 可选，空则同步到启动准点
}

// HandleRange POST /sync/range  按时间范围补数（异步）
func (a *SyncAPI) HandleRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 POST")
		return
	}
	if a.cfg == nil || !a.cfg.Ready {
		common.WriteFail(w, http.StatusOK, 1, "无配置运行中，不会做任何操作")
		return
	}
	m := store.Get()
	if m == nil || m.Syncer == nil {
		common.WriteFail(w, http.StatusOK, 2, "同步器未初始化")
		return
	}

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 3, err.Error())
		return
	}
	var req rangeReq
	if err := json.Unmarshal(raw, &req); err != nil {
		common.WriteFail(w, http.StatusBadRequest, 4, "请求体需 JSON：{start,end?}")
		return
	}
	// 兼容 query
	if req.Start == "" {
		req.Start = r.URL.Query().Get("start")
	}
	if req.End == "" {
		req.End = r.URL.Query().Get("end")
	}
	if req.Start == "" {
		common.WriteFail(w, http.StatusBadRequest, 5, "缺少 start，例：2026-08-27 00:00:00")
		return
	}

	start, err := time.ParseInLocation("2006-01-02 15:04:05", req.Start, time.Local)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 6, "start 格式错误，需 2006-01-02 15:04:05")
		return
	}
	var end time.Time
	if req.End != "" {
		end, err = time.ParseInLocation("2006-01-02 15:04:05", req.End, time.Local)
		if err != nil {
			common.WriteFail(w, http.StatusBadRequest, 7, "end 格式错误，需 2006-01-02 15:04:05")
			return
		}
	}

	info, err := m.Syncer.SyncRange(start, end)
	if err != nil {
		common.WriteFail(w, http.StatusOK, 8, err.Error())
		return
	}
	common.WriteOK(w, info)
}

// HandleStatus GET /sync/status
func (a *SyncAPI) HandleStatus(w http.ResponseWriter, r *http.Request) {
	m := store.Get()
	if m == nil || m.Syncer == nil {
		common.WriteOK(w, map[string]interface{}{"enabled": false})
		return
	}
	common.WriteOK(w, m.Syncer.Status())
}
