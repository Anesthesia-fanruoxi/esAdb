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

// SyncAPI 同步相关接口
type SyncAPI struct {
	cfg *config.Config
	mgr *store.Manager
}

func NewSyncAPI(cfg *config.Config, mgr *store.Manager) *SyncAPI {
	return &SyncAPI{cfg: cfg, mgr: mgr}
}

type timeReq struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

func (a *SyncAPI) parseTimeReq(r *http.Request) (timeReq, error) {
	var req timeReq
	raw, _ := io.ReadAll(r.Body)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return req, err
		}
	}
	if req.Start == "" {
		req.Start = r.URL.Query().Get("start")
	}
	if req.End == "" {
		req.End = r.URL.Query().Get("end")
	}
	return req, nil
}

func parseOptionalMs(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return common.ParseTimeInput(s)
}

// HandleBackfill GET/POST /sync/backfill  按时间范围补全同步
func (a *SyncAPI) HandleBackfill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 GET/POST")
		return
	}
	if a.mgr == nil || a.mgr.ES == nil || a.mgr.MySQL == nil {
		common.WriteFail(w, http.StatusServiceUnavailable, 503, "ES 或 MySQL 未就绪")
		return
	}

	req, err := a.parseTimeReq(r)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 1, "请求体格式错误")
		return
	}
	if req.Start == "" {
		common.WriteFail(w, http.StatusBadRequest, 2, "缺少 start")
		return
	}

	startMs, err := common.ParseTimeInput(req.Start)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 3, err.Error())
		return
	}
	endMs, err := parseOptionalMs(req.End)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 4, err.Error())
		return
	}

	plan, err := common.CalcBackfillPlan(startMs, endMs, a.cfg.Sync.Interval, a.cfg.Sync.LagSeconds, time.Now())
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 5, err.Error())
		return
	}

	common.Info("[backfill] 开始补全 totalWindows=%d range=[%s, %s)",
		plan.TotalWindows, plan.RangeStart.Time, plan.RangeEnd.Time)

	summary := a.mgr.BackfillWindows(plan.Windows, plan.RangeStart.Time, plan.RangeEnd.Time)
	common.Info("[backfill] 完成 hits=%d written=%d failed=%d",
		summary.TotalHits, summary.TotalWritten, summary.Failed)

	common.WriteOK(w, map[string]interface{}{
		"plan":    plan,
		"summary": summary,
	})
}

// HandleCompare GET/POST /sync/compare  对比 ES 与 ADB 条数
func (a *SyncAPI) HandleCompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 GET/POST")
		return
	}
	if a.mgr == nil || a.mgr.ES == nil || a.mgr.MySQL == nil {
		common.WriteFail(w, http.StatusServiceUnavailable, 503, "ES 或 MySQL 未就绪")
		return
	}

	req, err := a.parseTimeReq(r)
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 1, "请求体格式错误")
		return
	}

	var startMs, endMs int64
	if req.Start != "" {
		startMs, err = common.ParseTimeInput(req.Start)
		if err != nil {
			common.WriteFail(w, http.StatusBadRequest, 2, err.Error())
			return
		}
	}
	if req.End != "" {
		endMs, err = parseOptionalMs(req.End)
		if err != nil {
			common.WriteFail(w, http.StatusBadRequest, 3, err.Error())
			return
		}
	}

	win, err := common.CalcCompareRange(startMs, endMs, a.cfg.Sync.Interval, a.cfg.Sync.LagSeconds, time.Now())
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 4, err.Error())
		return
	}

	result, err := a.mgr.CompareRange(win)
	if err != nil {
		common.WriteFail(w, http.StatusInternalServerError, 5, err.Error())
		return
	}
	common.Info("[compare] [%s, %s) ES=%d ADB=%d diff=%d",
		win.Start, win.End, result.ES.Count, result.MySQL.Count, result.Diff)
	common.WriteOK(w, result)
}
