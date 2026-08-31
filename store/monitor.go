package store

import (
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"esAdb/common"
)

const (
	monitorRetention    = time.Hour
	sessionQpsMax       = 300  // 本次补全 QPS/运行时序列上限（2s 采样 ≈ 最近 10 分钟）
	backfillWindowsMax  = 2000 // 补全单窗口采样点上限（防整月数十万窗口无限累积内存）
	backfillPointsMax   = 4000 // 补全进度快照上限
)

// IncrementalPoint 单次增量同步记录
type IncrementalPoint struct {
	At         int64              `json:"at"`
	AtStr      string             `json:"atStr"`
	Window     common.TimeRangeMs `json:"window"`
	Hits       int                `json:"hits"`
	Written    int                `json:"written"`
	DurationMs int64              `json:"durationMs"`
	Success    bool               `json:"success"`
	Error      string             `json:"error,omitempty"`
}

// BackfillWindowPoint 补全单窗口记录
type BackfillWindowPoint struct {
	At      int64              `json:"at"`
	AtStr   string             `json:"atStr"`
	Window  common.TimeRangeMs `json:"window"`
	Hits    int                `json:"hits"`
	Written int                `json:"written"`
	Success bool               `json:"success"`
	Error   string             `json:"error,omitempty"`
}

// BackfillProgressPoint 补全进度快照（主页 SSE / 侧栏）
type BackfillProgressPoint struct {
	At           int64   `json:"at"`
	AtStr        string  `json:"atStr"`
	TotalWindows int     `json:"totalWindows"`
	Completed    int     `json:"completed"`
	Failed       int     `json:"failed"`
	TotalHits    int     `json:"totalHits"`
	TotalWritten int     `json:"totalWritten"`
	Percent      float64 `json:"percent"`
	RangeStart   string  `json:"rangeStart,omitempty"`
	RangeEnd     string  `json:"rangeEnd,omitempty"`
}

// QpsPoint 本次补全会话每秒 QPS 采样点
type QpsPoint struct {
	At        int64   `json:"at"`
	AtStr     string  `json:"atStr"`
	WriteQPS  float64 `json:"writeQps"`
	WindowQPS float64 `json:"windowQps"`
	HitQPS    float64 `json:"hitQps"`
}

// RuntimePoint 本次补全会话服务运行时序列点
type RuntimePoint struct {
	At           int64   `json:"at"`
	AtStr        string  `json:"atStr"`
	HeapAllocMB  float64 `json:"heapAllocMB"`
	HeapSysMB    float64 `json:"heapSysMB"`
	GcPerSec     float64 `json:"gcPerSec"`  // 每秒 GC 次数（相邻采样差值）
	NumGoroutine int     `json:"numGoroutine"`
	AvgWindowMs  float64 `json:"avgWindowMs"` // 上个采样间隔内窗口平均耗时（毫秒）
}

// BackfillSessionMeta 本次补全会话元信息（详情弹框「本次进度」）
type BackfillSessionMeta struct {
	StartedAtMs   int64              `json:"startedAtMs"`
	StartedAtStr  string             `json:"startedAtStr"`
	FinishedAtMs  int64              `json:"finishedAtMs,omitempty"`
	FinishedAtStr string             `json:"finishedAtStr,omitempty"`
	FirstWindow   common.TimeRangeMs `json:"firstWindow,omitempty"`
	LastWindow    common.TimeRangeMs `json:"lastWindow,omitempty"`
}

// BackfillDetail 详情 SSE 载荷（GET /monitor/backfill/sse）
type BackfillDetail struct {
	BackfillActive bool                   `json:"backfillActive"`
	Progress       *BackfillProgressPoint `json:"progress"`
	Session        BackfillSessionMeta    `json:"session"`
	QpsSeries      []QpsPoint             `json:"qpsSeries"`
	RuntimeSeries  []RuntimePoint         `json:"runtimeSeries"`
}

// PipelineSnapshot 接线图实时状态
type PipelineSnapshot struct {
	At                 int64                  `json:"at"`
	AtStr              string                 `json:"atStr"`
	EsReady            bool                   `json:"esReady"`
	MysqlReady         bool                   `json:"mysqlReady"`
	IncrementalRunning bool                   `json:"incrementalRunning"`
	IntervalSec        int                    `json:"intervalSec"`
	LagSec             int                    `json:"lagSec"`
	BackfillWorkers    int                    `json:"backfillWorkers"`
	BackfillPauseMs    int                    `json:"backfillPauseMs"`
	NumGoroutine       int                    `json:"numGoroutine"`
	HeapAllocMB        float64                `json:"heapAllocMB"`
	TargetWindow       *common.TimeRangeMs    `json:"targetWindow,omitempty"`
	LastIncremental    *IncrementalPoint      `json:"lastIncremental,omitempty"`
	BackfillActive     bool                   `json:"backfillActive"`
	BackfillProgress   *BackfillProgressPoint `json:"backfillProgress,omitempty"`
}

// MonitorHistory 最近 1 小时历史（连接 SSE 时首包）
type MonitorHistory struct {
	RetentionSec int                     `json:"retentionSec"`
	Incremental  []IncrementalPoint      `json:"incremental"`
	Backfill     []BackfillProgressPoint `json:"backfill"`
	Windows      []BackfillWindowPoint   `json:"backfillWindows"`
}

// SSEMessage 推送给前端的事件
type SSEMessage struct {
	Event string
	Data  interface{}
}

type monitorJob struct {
	fn func()
}

// Monitor 增量/补全监控，内存保留 1 小时
type Monitor struct {
	mgr *Manager

	mu               sync.RWMutex
	incremental      []IncrementalPoint
	backfillProgress []BackfillProgressPoint
	backfillWindows  []BackfillWindowPoint
	sessionQps       []QpsPoint
	sessionRuntime   []RuntimePoint
	qpsSampleAt      time.Time
	qpsSampleDone    int
	qpsSampleHits    int
	qpsSampleWritten int
	sessionFirstWin  common.TimeRangeMs
	sessionLastWin   common.TimeRangeMs
	sessionFinished  time.Time
	lastIncremental  *IncrementalPoint
	backfillActive   bool
	currentBackfill  *BackfillProgressPoint
	backfillStarted  time.Time
	rtLastNumGC uint32 // 上次采样的 GC 累计值（用于计算每秒速率）
	bfWinCount  int64  // 上次采样以来完成的补全窗口数（算平均耗时用）
	bfDurSumMs  int64  // 上次采样以来窗口耗时总和（毫秒）

	jobCh  chan monitorJob
	subsMu sync.RWMutex
	subs   map[int]chan SSEMessage
	subSeq int
	stopCh chan struct{}
}

func NewMonitor(mgr *Manager) *Monitor {
	return &Monitor{
		mgr:    mgr,
		jobCh:  make(chan monitorJob, 512),
		subs:   make(map[int]chan SSEMessage),
		stopCh: make(chan struct{}),
	}
}

// Start 启动记录与过期清理 goroutine
func (mon *Monitor) Start() {
	go mon.loop()
}

func (mon *Monitor) loop() {
	pruneTicker := time.NewTicker(time.Minute)
	defer pruneTicker.Stop()

	for {
		select {
		case <-mon.stopCh:
			return
		case job := <-mon.jobCh:
			job.fn()
		case <-pruneTicker.C:
			mon.enqueue(func() { mon.prune(time.Now()) })
		}
	}
}

func (mon *Monitor) enqueue(fn func()) {
	select {
	case mon.jobCh <- monitorJob{fn: fn}:
	default:
		go func() { mon.jobCh <- monitorJob{fn: fn} }()
	}
}

func (mon *Monitor) prune(now time.Time) {
	cutoff := now.Add(-monitorRetention).UnixMilli()
	mon.mu.Lock()
	defer mon.mu.Unlock()
	mon.incremental = filterIncremental(mon.incremental, cutoff)
	mon.backfillProgress = filterBackfillProgress(mon.backfillProgress, cutoff)
	mon.backfillWindows = filterBackfillWindows(mon.backfillWindows, cutoff)
}

func filterIncremental(in []IncrementalPoint, cutoff int64) []IncrementalPoint {
	out := in[:0]
	for _, p := range in {
		if p.At >= cutoff {
			out = append(out, p)
		}
	}
	return out
}

func filterBackfillProgress(in []BackfillProgressPoint, cutoff int64) []BackfillProgressPoint {
	out := in[:0]
	for _, p := range in {
		if p.At >= cutoff {
			out = append(out, p)
		}
	}
	return out
}

func filterBackfillWindows(in []BackfillWindowPoint, cutoff int64) []BackfillWindowPoint {
	out := in[:0]
	for _, p := range in {
		if p.At >= cutoff {
			out = append(out, p)
		}
	}
	return out
}

func (mon *Monitor) broadcast(msg SSEMessage) {
	mon.subsMu.RLock()
	defer mon.subsMu.RUnlock()
	for _, ch := range mon.subs {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Subscribe SSE 订阅
func (mon *Monitor) Subscribe() (id int, ch <-chan SSEMessage) {
	c := make(chan SSEMessage, 32)
	mon.subsMu.Lock()
	mon.subSeq++
	id = mon.subSeq
	mon.subs[id] = c
	mon.subsMu.Unlock()
	return id, c
}

// Unsubscribe 取消订阅
func (mon *Monitor) Unsubscribe(id int) {
	mon.subsMu.Lock()
	if ch, ok := mon.subs[id]; ok {
		delete(mon.subs, id)
		close(ch)
	}
	mon.subsMu.Unlock()
}

// History 返回 1 小时内历史快照
func (mon *Monitor) History() MonitorHistory {
	mon.mu.RLock()
	defer mon.mu.RUnlock()
	return MonitorHistory{
		RetentionSec: int(monitorRetention.Seconds()),
		Incremental:  append([]IncrementalPoint(nil), mon.incremental...),
		Backfill:     append([]BackfillProgressPoint(nil), mon.backfillProgress...),
		Windows:      append([]BackfillWindowPoint(nil), mon.backfillWindows...),
	}
}

type pipelineSvc struct {
	EsReady            bool
	MysqlReady         bool
	IncrementalRunning bool
	IntervalSec        int
	LagSec             int
	BackfillWorkers    int
	BackfillPauseMs    int
	NumGoroutine       int
	HeapAllocMB        float64
}

func (mon *Monitor) pipelineSvc() pipelineSvc {
	svc := pipelineSvc{NumGoroutine: runtime.NumGoroutine()}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	svc.HeapAllocMB = round1(float64(ms.Alloc) / (1 << 20))
	if mon.mgr != nil {
		svc.EsReady = mon.mgr.ES != nil
		svc.MysqlReady = mon.mgr.MySQL != nil
		if mon.mgr.cfg != nil {
			svc.IntervalSec = mon.mgr.cfg.Sync.Interval
			svc.LagSec = mon.mgr.cfg.Sync.LagSeconds
			svc.BackfillWorkers = mon.mgr.cfg.Sync.BackfillWorkers
			svc.BackfillPauseMs = mon.mgr.cfg.Sync.BackfillPauseMs
		}
		if mon.mgr.Syncer != nil {
			svc.IncrementalRunning = mon.mgr.Syncer.incrRunning
		}
	}
	return svc
}

// Pipeline 当前接线图状态
func (mon *Monitor) Pipeline() PipelineSnapshot {
	now := time.Now()
	svc := mon.pipelineSvc()
	snap := PipelineSnapshot{
		At:                 now.UnixMilli(),
		AtStr:              common.FormatMs(now.UnixMilli()),
		EsReady:            svc.EsReady,
		MysqlReady:         svc.MysqlReady,
		IncrementalRunning: svc.IncrementalRunning,
		IntervalSec:        svc.IntervalSec,
		LagSec:             svc.LagSec,
		BackfillWorkers:    svc.BackfillWorkers,
		BackfillPauseMs:    svc.BackfillPauseMs,
		NumGoroutine:       svc.NumGoroutine,
		HeapAllocMB:        svc.HeapAllocMB,
	}

	if mon.mgr != nil {
		win := common.IncrementalWindow(now, snap.IntervalSec, snap.LagSec)
		snap.TargetWindow = &win
	}
	mon.mu.RLock()
	if mon.lastIncremental != nil {
		c := *mon.lastIncremental
		snap.LastIncremental = &c
	}
	snap.BackfillActive = mon.backfillActive
	if mon.currentBackfill != nil {
		c := *mon.currentBackfill
		snap.BackfillProgress = &c
	}
	mon.mu.RUnlock()
	return snap
}

// BuildBackfillDetail 组装补全详情 SSE 载荷（弹框 SSE 约 1Hz 采样 QPS 与服务指标）
func (mon *Monitor) BuildBackfillDetail() BackfillDetail {
	mon.mu.Lock()
	mon.appendSessionSamplesLocked()
	qps := append([]QpsPoint(nil), mon.sessionQps...)
	runtimeSeries := append([]RuntimePoint(nil), mon.sessionRuntime...)
	var progress *BackfillProgressPoint
	if mon.currentBackfill != nil {
		c := *mon.currentBackfill
		progress = &c
	}
	backfillActive := mon.backfillActive
	session := mon.buildSessionMetaLocked()
	mon.mu.Unlock()

	return BackfillDetail{
		BackfillActive: backfillActive,
		Progress:       progress,
		Session:        session,
		QpsSeries:      qps,
		RuntimeSeries:  runtimeSeries,
	}
}

func (mon *Monitor) buildSessionMetaLocked() BackfillSessionMeta {
	meta := BackfillSessionMeta{}
	if !mon.backfillStarted.IsZero() {
		meta.StartedAtMs = mon.backfillStarted.UnixMilli()
		meta.StartedAtStr = common.FormatMs(meta.StartedAtMs)
	}
	if !mon.sessionFinished.IsZero() {
		meta.FinishedAtMs = mon.sessionFinished.UnixMilli()
		meta.FinishedAtStr = common.FormatMs(meta.FinishedAtMs)
	}
	if mon.sessionFirstWin.Start != "" {
		meta.FirstWindow = mon.sessionFirstWin
	}
	if mon.sessionLastWin.Start != "" {
		meta.LastWindow = mon.sessionLastWin
	}
	return meta
}

func (mon *Monitor) appendSessionSamplesLocked() {
	now := time.Now()
	at := now.UnixMilli()
	if n := len(mon.sessionRuntime); n > 0 && at-mon.sessionRuntime[n-1].At < 2000 {
		return
	}

	var writeQps, hitQps, windowQps float64
	if mon.currentBackfill != nil {
		done := mon.currentBackfill.Completed + mon.currentBackfill.Failed
		hits := mon.currentBackfill.TotalHits
		written := mon.currentBackfill.TotalWritten
		if !mon.qpsSampleAt.IsZero() {
			sec := now.Sub(mon.qpsSampleAt).Seconds()
			if sec < 0.001 {
				sec = 0.001
			}
			dDone := done - mon.qpsSampleDone
			dHits := hits - mon.qpsSampleHits
			dWrt := written - mon.qpsSampleWritten
			if dDone < 0 {
				dDone = 0
			}
			if dHits < 0 {
				dHits = 0
			}
			if dWrt < 0 {
				dWrt = 0
			}
			writeQps = round1(float64(dWrt) / sec)
			hitQps = round1(float64(dHits) / sec)
			windowQps = round1(float64(dDone) / sec)
		}
		mon.qpsSampleAt = now
		mon.qpsSampleDone = done
		mon.qpsSampleHits = hits
		mon.qpsSampleWritten = written
	}
	mon.sessionQps = append(mon.sessionQps, QpsPoint{
		At:        at,
		AtStr:     common.FormatMs(at),
		WriteQPS:  writeQps,
		WindowQPS: windowQps,
		HitQPS:    hitQps,
	})
	if len(mon.sessionQps) > sessionQpsMax {
		mon.sessionQps = mon.sessionQps[len(mon.sessionQps)-sessionQpsMax:]
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	var gcPerSec float64
	if n := len(mon.sessionRuntime); n > 0 {
		sec := float64(at-mon.sessionRuntime[n-1].At) / 1000
		if sec < 0.001 {
			sec = 0.001
		}
		dGC := int64(ms.NumGC - mon.rtLastNumGC) // uint32 回绕安全
		if dGC < 0 {
			dGC = 0
		}
		gcPerSec = round1(float64(dGC) / sec)
	}
	mon.rtLastNumGC = ms.NumGC
	var avgWinMs float64
	if mon.bfWinCount > 0 {
		avgWinMs = round1(float64(mon.bfDurSumMs) / float64(mon.bfWinCount))
		mon.bfWinCount = 0
		mon.bfDurSumMs = 0
	}
	mon.sessionRuntime = append(mon.sessionRuntime, RuntimePoint{
		At:           at,
		AtStr:        common.FormatMs(at),
		HeapAllocMB:  round1(float64(ms.Alloc) / (1 << 20)),
		HeapSysMB:    round1(float64(ms.HeapSys) / (1 << 20)),
		GcPerSec:     gcPerSec,
		NumGoroutine: runtime.NumGoroutine(),
		AvgWindowMs:  avgWinMs,
	})
	if len(mon.sessionRuntime) > sessionQpsMax {
		mon.sessionRuntime = mon.sessionRuntime[len(mon.sessionRuntime)-sessionQpsMax:]
	}
}

// RecordIncremental 记录增量写入（异步）
func (mon *Monitor) RecordIncremental(win common.TimeRangeMs, hits, written int, dur time.Duration, err error) {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		now := time.Now()
		pt := IncrementalPoint{
			At:         now.UnixMilli(),
			AtStr:      common.FormatMs(now.UnixMilli()),
			Window:     win,
			Hits:       hits,
			Written:    written,
			DurationMs: dur.Milliseconds(),
			Success:    err == nil,
		}
		if err != nil {
			pt.Error = err.Error()
		}
		mon.mu.Lock()
		mon.incremental = append(mon.incremental, pt)
		c := pt
		mon.lastIncremental = &c
		mon.mu.Unlock()
		mon.prune(now)
		mon.broadcast(SSEMessage{Event: "incremental", Data: pt})
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
	})
}

// BeginBackfill 补全开始
func (mon *Monitor) BeginBackfill(rangeStart, rangeEnd string, totalWindows int, firstWin, lastWin common.TimeRangeMs) {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		now := time.Now()
		pt := BackfillProgressPoint{
			At:           now.UnixMilli(),
			AtStr:        common.FormatMs(now.UnixMilli()),
			TotalWindows: totalWindows,
			RangeStart:   rangeStart,
			RangeEnd:     rangeEnd,
		}
		mon.mu.Lock()
		mon.backfillActive = true
		mon.backfillStarted = now
		mon.sessionFinished = time.Time{}
		mon.sessionQps = mon.sessionQps[:0]
		mon.sessionRuntime = mon.sessionRuntime[:0]
		mon.rtLastNumGC = 0
		mon.bfWinCount = 0
		mon.bfDurSumMs = 0
		mon.qpsSampleAt = time.Time{}
		mon.qpsSampleDone = 0
		mon.qpsSampleHits = 0
		mon.qpsSampleWritten = 0
		mon.sessionFirstWin = firstWin
		mon.sessionLastWin = lastWin
		mon.currentBackfill = &pt
		mon.backfillProgress = append(mon.backfillProgress, pt)
		if n := len(mon.backfillProgress); n > backfillPointsMax {
			mon.backfillProgress = mon.backfillProgress[n-backfillPointsMax:]
		}
		mon.mu.Unlock()
		mon.broadcast(SSEMessage{Event: "backfill", Data: pt})
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
	})
}

// RecordBackfillWindow 补全单窗口完成
func (mon *Monitor) RecordBackfillWindow(res WindowSyncResult) {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		now := time.Now()
		pt := BackfillWindowPoint{
			At:      now.UnixMilli(),
			AtStr:   common.FormatMs(now.UnixMilli()),
			Window:  res.Window,
			Hits:    res.Hits,
			Written: res.Written,
			Success: res.Error == "",
		}
		if res.Error != "" {
			pt.Error = res.Error
		}
		mon.mu.Lock()
		mon.backfillWindows = append(mon.backfillWindows, pt)
		if n := len(mon.backfillWindows); n > backfillWindowsMax {
			mon.backfillWindows = mon.backfillWindows[n-backfillWindowsMax:]
		}
		mon.bfWinCount++
		mon.bfDurSumMs += res.DurationMs
		mon.mu.Unlock()
		mon.prune(now)
		mon.broadcast(SSEMessage{Event: "backfill_window", Data: pt})
	})
}

// UpdateBackfillProgress 更新补全进度
func (mon *Monitor) UpdateBackfillProgress(completed, failed, totalHits, totalWritten, totalWindows int, rangeStart, rangeEnd string) {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		now := time.Now()
		done := completed + failed
		var pct float64
		if totalWindows > 0 {
			pct = float64(done) / float64(totalWindows) * 100
		}
		if pct > 100 {
			pct = 100
		}

		pt := BackfillProgressPoint{
			At:           now.UnixMilli(),
			AtStr:        common.FormatMs(now.UnixMilli()),
			TotalWindows: totalWindows,
			Completed:    completed,
			Failed:       failed,
			TotalHits:    totalHits,
			TotalWritten: totalWritten,
			Percent:      pct,
			RangeStart:   rangeStart,
			RangeEnd:     rangeEnd,
		}
		mon.mu.Lock()
		if mon.backfillStarted.IsZero() {
			mon.backfillStarted = now
		}
		mon.currentBackfill = &pt
		mon.backfillProgress = append(mon.backfillProgress, pt)
		if n := len(mon.backfillProgress); n > backfillPointsMax {
			mon.backfillProgress = mon.backfillProgress[n-backfillPointsMax:]
		}
		mon.mu.Unlock()

		mon.prune(now)
		mon.broadcast(SSEMessage{Event: "backfill", Data: pt})
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
	})
}

func round1(v float64) float64 {
	return float64(int(v*10+0.5)) / 10
}

// EndBackfill 补全结束
func (mon *Monitor) EndBackfill() {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		now := time.Now()
		mon.mu.Lock()
		mon.backfillActive = false
		if !mon.backfillStarted.IsZero() {
			mon.sessionFinished = now
		}
		mon.mu.Unlock()
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
		// 归还空闲内存给 OS（HeapSys/Sys 指标不回落，但实际占用立即下降）
		go debug.FreeOSMemory()
	})
}
