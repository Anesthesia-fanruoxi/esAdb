package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"esAdb/common"
	"esAdb/config"
	"esAdb/store"
)

// SyncAPI 同步相关接口
type SyncAPI struct {
	cfg     *config.Config
	mgr     *store.Manager
	bfMu    sync.Mutex // 补全任务互斥：同一时刻仅允许一个补全任务
	drillMu sync.Mutex // 异常分析互斥：同一时刻仅允许一个下钻分析任务
}

func NewSyncAPI(cfg *config.Config, mgr *store.Manager) *SyncAPI {
	return &SyncAPI{cfg: cfg, mgr: mgr}
}

type windowRangeReq struct {
	StartMs int64 `json:"startMs"`
	EndMs   int64 `json:"endMs"`
}

type timeReq struct {
	Start   string           `json:"start"`
	End     string           `json:"end"`
	Windows []windowRangeReq `json:"windows"`
}

// winBrief 下钻 done 事件中一次性下发的异常窗口（分析中不下发，减轻 SSE 压力）
type winBrief struct {
	Level int    `json:"level"`
	S     int64  `json:"s"`
	E     int64  `json:"e"`
	Start string `json:"start"`
	End   string `json:"end"`
	Diff  int    `json:"diff"`
}

// HandleBackfill GET/POST /sync/backfill  范围补全（全量分页模式）
// 入参：{ start, end }（可含 query），整段范围 search_after 翻页补全，每批 backfill_batch 条。
// 窗口补全请使用 /sync/backfill/windows。
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

	// 边界对齐（与窗口补全同规则）：start 向下、end 向上；无 end 则截止到 lag 边界
	intervalMs := common.IntervalMs(a.cfg.Sync.Interval)
	rangeStartMs := common.AlignFloorMs(startMs, intervalMs)
	var rangeEndMs int64
	if endMs > 0 {
		rangeEndMs = common.AlignCeilMs(endMs, intervalMs)
	} else {
		_, rangeEndMs = common.PrevWindowMs(time.Now().UnixMilli()-common.LagMs(a.cfg.Sync.LagSeconds), intervalMs)
	}
	if rangeEndMs <= rangeStartMs {
		common.WriteFail(w, http.StatusBadRequest, 5, "时间范围无效")
		return
	}

	// 预估批次数（进度分母，实际以补全为准）
	batch := a.cfg.Sync.BackfillBatch
	totalDocs, _ := a.mgr.ES.CountByRangeMs(rangeStartMs, rangeEndMs)
	estBatches := 1
	if totalDocs > 0 {
		estBatches = (totalDocs + batch - 1) / batch
	}
	plan := map[string]interface{}{
		"hasEnd":       endMs > 0,
		"mode":         "paged",
		"batch":        batch,
		"totalDocs":    totalDocs,
		"totalWindows": estBatches, // 兼容前端字段：此处为预估批次数
		"rangeStart":   common.NewTimePointMs(rangeStartMs),
		"rangeEnd":     common.NewTimePointMs(rangeEndMs),
		"firstWindow":  common.NewTimeRangeMs(rangeStartMs, rangeEndMs),
		"lastWindow":   common.NewTimeRangeMs(rangeStartMs, rangeEndMs),
	}

	if !a.bfMu.TryLock() {
		common.Warn("[backfill] 存在进行中的补全任务，本次范围补全被拒绝")
		common.WriteFail(w, http.StatusConflict, 20, "已有补全任务在执行，请等待其完成后再试")
		return
	}
	defer a.bfMu.Unlock()

	common.Info("[backfill] 开始范围补全(分页) estBatches=%d totalDocs=%d range=[%s, %s)",
		estBatches, totalDocs, common.FormatMs(rangeStartMs), common.FormatMs(rangeEndMs))

	summary := a.mgr.BackfillRangePaged(rangeStartMs, rangeEndMs, batch, estBatches)
	common.Info("[backfill] 完成 hits=%d written=%d failed=%d",
		summary.TotalHits, summary.TotalWritten, summary.Failed)

	common.WriteOK(w, map[string]interface{}{
		"plan":    plan,
		"summary": summary,
	})
}

// HandleBackfillWindows POST /sync/backfill/windows  窗口补全（支持多个不连续窗口）
// 入参：{ windows:[{startMs,endMs},...] }，直接按给定窗口补全，不做切窗。
func (a *SyncAPI) HandleBackfillWindows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		common.WriteFail(w, http.StatusMethodNotAllowed, 405, "仅支持 POST")
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
	if len(req.Windows) == 0 {
		common.WriteFail(w, http.StatusBadRequest, 11, "缺少 windows")
		return
	}
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

	if !a.bfMu.TryLock() {
		common.Warn("[backfill] 存在进行中的补全任务，本次窗口补全被拒绝")
		common.WriteFail(w, http.StatusConflict, 20, "已有补全任务在执行，请等待其完成后再试")
		return
	}
	defer a.bfMu.Unlock()

	common.Info("[backfill] 按窗口补全 n=%d range=[%s, %s)", len(windows), rangeStart, rangeEnd)

	summary := a.mgr.BackfillWindows(windows, rangeStart, rangeEnd)
	common.Info("[backfill] 完成 hits=%d written=%d failed=%d",
		summary.TotalHits, summary.TotalWritten, summary.Failed)
	common.WriteOK(w, map[string]interface{}{
		"windows": len(windows),
		"summary": summary,
	})
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
// 金字塔逐级下钻找出异常时间窗（默认 日 → 小时 → 5分钟 → 10 秒）。
// 分析过程中仅逐级回传数量事件 levelN（驱动前端 4 段进度条，不下发明细以减轻 SSE 压力），
// 全部完成后于 done 一次性下发全量异常窗口。
// query: start, end, workers(默认12), 可选 l1/l2/l3/l4 为秒，覆盖各级粒度
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
	// 各级粒度（秒），默认 日 / 小时 / 5分钟 / 10秒（金字塔剪枝，支持对比整月）。
	// 传 0 表示本层不下钻（只跑前几层）
	parseLvl := func(key string, def int) int {
		if v := r.URL.Query().Get(key); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				return n
			}
		}
		return def
	}
	levelSecs := make([]int64, 0, 4)
	for _, kv := range []struct {
		k   string
		def int
	}{{"l1", 86400}, {"l2", 3600}, {"l3", 300}, {"l4", 10}} {
		if n := parseLvl(kv.k, kv.def); n > 0 {
			levelSecs = append(levelSecs, int64(n))
		}
	}
	if len(levelSecs) == 0 {
		levelSecs = []int64{86400, 3600, 300, 10}
	}
	levelMs := make([]int64, len(levelSecs))
	for i, s := range levelSecs {
		levelMs[i] = s * 1000
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

	if !a.drillMu.TryLock() {
		common.Warn("[drilldown] 存在进行中的异常分析任务，本次下钻被拒绝")
		common.WriteFail(w, http.StatusConflict, 21, "已有异常分析任务在执行，请等待其完成后再试")
		return
	}
	defer a.drillMu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	var sendMu sync.Mutex
	send := func(event string, data interface{}) bool {
		b, err := common.MarshalJSON(data, false)
		if err != nil {
			common.Warn("[drilldown] SSE 序列化失败 event=%s: %s", event, err.Error())
			return false
		}
		sendMu.Lock()
		defer sendMu.Unlock()
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
			common.Warn("[drilldown] SSE 写入失败 event=%s: %s", event, err.Error())
			return false
		}
		flusher.Flush()
		return true
	}

	common.Info("[drilldown] 下钻开始 range=[%s, %s) workers=%d l1=%ds l2=%ds l3=%ds",
		win.Start, win.End, workers, levelSecs[0], levelSecs[1], levelSecs[2])

	if !send("range", win) {
		common.Warn("[drilldown] range 事件发送失败，连接可能已断开")
		return
	}
	common.Info("[drilldown] range 事件已发送")

	// 每个窗口算完即推送一次进度计数 progress {level,done,total}（不含窗口明细），
	// 前端据此按「(第level-1级)+done/total」换算成 4 段 25% 的精确进度；
	// 全部完成后再于 done 一次性下发全量异常窗口，最大化减少 SSE 明细传输。
	levels, err := a.mgr.DrilldownLevels(r.Context(), win, levelMs, workers, nil,
		func(level, done, total int) {
			// 每个窗口算完即推送一次进度计数，只带数量、不含窗口明细；
			// 不在此处写日志，避免第 4 级(可达数千窗)刷屏
			send("progress", map[string]int{"level": level, "done": done, "total": total})
		})
	if err != nil {
		common.Error("[drilldown] 下钻失败: %s", err.Error())
		send("error", map[string]string{"message": err.Error()})
		return
	}
	lastAbnormal := 0
	wins := make([]winBrief, 0, 8)
	if len(levels) > 0 {
		lastAbnormal = levels[len(levels)-1].Abnormal
		for _, lv := range levels {
			for i := range lv.Windows {
				r := lv.Windows[i]
				wins = append(wins, winBrief{Level: lv.Level, S: r.Range.StartMs, E: r.Range.EndMs, Start: r.Range.Start, End: r.Range.End, Diff: r.Diff})
			}
		}
	}
	common.Info("[drilldown] 下钻完成 abnormal=%d windows=%d", lastAbnormal, len(wins))
	send("done", map[string]interface{}{
		"abnormal": lastAbnormal,
		"windows":  wins,
	})
}
