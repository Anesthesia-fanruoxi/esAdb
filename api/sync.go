package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
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

type windowRangeReq struct {
	StartMs int64 `json:"startMs"`
	EndMs   int64 `json:"endMs"`
}

type timeReq struct {
	Start   string          `json:"start"`
	End     string          `json:"end"`
	Windows []windowRangeReq `json:"windows"`
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

// HandleBackfill GET/POST /sync/backfill  补全同步
// 支持两种入参（二选一）：
//  1) 连续范围：{ start, end }（可含 query），内部切窗后补全
//  2) 按窗口列表（不连续，用于补全三级下钻定位的异常窗口）：{ windows:[{startMs,endMs}] }
// 两种方式最终都走 store.BackfillWindows，仅入参切窗方式不同
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

	// 方式二：按显式窗口列表补全（不连续）
	if len(req.Windows) > 0 {
		if len(req.Windows) > 4000 {
			common.WriteFail(w, http.StatusBadRequest, 12, "单次补全窗口数不能超过 4000")
			return
		}
		seen := make(map[int64]bool, len(req.Windows))
		windows := make([]common.TimeRangeMs, 0, len(req.Windows))
		for _, wr := range req.Windows {
			if wr.EndMs <= wr.StartMs {
				common.WriteFail(w, http.StatusBadRequest, 13, "窗口无效: endMs 需大于 startMs")
				return
			}
			if seen[wr.StartMs] {
				continue
			}
			seen[wr.StartMs] = true
			windows = append(windows, common.NewTimeRangeMs(wr.StartMs, wr.EndMs))
		}
		if len(windows) == 0 {
			common.WriteFail(w, http.StatusBadRequest, 14, "windows 去重后为空")
			return
		}
		sort.Slice(windows, func(i, j int) bool { return windows[i].StartMs < windows[j].StartMs })

		rangeStart := common.FormatMs(windows[0].StartMs)
		rangeEnd := common.FormatMs(windows[len(windows)-1].EndMs)
		common.Info("[backfill] 按窗口列表补全 n=%d range=[%s, %s)", len(windows), rangeStart, rangeEnd)

		summary := a.mgr.BackfillWindows(windows, rangeStart, rangeEnd)
		common.Info("[backfill] 完成 hits=%d written=%d failed=%d",
			summary.TotalHits, summary.TotalWritten, summary.Failed)
		common.WriteOK(w, map[string]interface{}{
			"windows": len(windows),
			"summary": summary,
		})
		return
	}

	// 方式一：连续范围补全
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

// HandleCompareDrilldownSSE GET /sync/compare/drilldown/sse
// 三级下钻找出异常时间窗（默认 小时 → 分钟 → 10 秒），逐级以 SSE 流式返回
// query: start, end, workers(默认12), 可选 l1/l2/l3 为秒，覆盖各级粒度
func (a *SyncAPI) HandleCompareDrilldownSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 GET")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		common.WriteFail(w, http.StatusInternalServerError, 500, "不支持 SSE")
		return
	}
	if a.mgr == nil || a.mgr.ES == nil || a.mgr.MySQL == nil {
		common.WriteFail(w, http.StatusServiceUnavailable, 503, "ES 或 MySQL 未就绪")
		return
	}

	parseIntQ := func(key string, def int) int {
		if v := r.URL.Query().Get(key); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return n
			}
		}
		return def
	}
	workers := parseIntQ("workers", 12)
	levelSecs := []int64{
		int64(parseIntQ("l1", 3600)),
		int64(parseIntQ("l2", 60)),
		int64(parseIntQ("l3", 10)),
	}

	startMs, err := parseOptionalMs(r.URL.Query().Get("start"))
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 1, err.Error())
		return
	}
	endMs, err := parseOptionalMs(r.URL.Query().Get("end"))
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 2, err.Error())
		return
	}
	win, err := common.CalcCompareRange(startMs, endMs, a.cfg.Sync.Interval, a.cfg.Sync.LagSeconds, time.Now())
	if err != nil {
		common.WriteFail(w, http.StatusBadRequest, 3, err.Error())
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	send := func(event string, data interface{}) bool {
		b, err := common.MarshalJSON(data, false)
		if err != nil {
			return false
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	common.Info("[drilldown] 下钻开始 range=[%s, %s) workers=%d l1=%ds l2=%ds l3=%ds",
		win.Start, win.End, workers, levelSecs[0], levelSecs[1], levelSecs[2])

	if !send("range", win) {
		return
	}

	levels, err := a.mgr.DrilldownLevels(r.Context(), win, levelSecs, workers)
	if err != nil {
		send("error", map[string]string{"message": err.Error()})
		return
	}
	for _, lv := range levels {
		if !send(fmt.Sprintf("level%d", lv.Level), lv) {
			return
		}
	}
	lastAbnormal := 0
	if len(levels) > 0 {
		lastAbnormal = levels[len(levels)-1].Abnormal
	}
	send("done", map[string]int{"abnormal": lastAbnormal})
}
