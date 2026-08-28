package store

import (
	"sync"
	"time"

	"esAdb/common"
)

const monitorRetention = time.Hour

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

// BackfillProgressPoint 补全进度快照
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

// PipelineSnapshot 接线图实时状态
type PipelineSnapshot struct {
	At                 int64                  `json:"at"`
	AtStr              string                 `json:"atStr"`
	EsReady            bool                   `json:"esReady"`
	MysqlReady         bool                   `json:"mysqlReady"`
	IncrementalRunning bool                   `json:"incrementalRunning"`
	IntervalSec        int                    `json:"intervalSec"`
	LagSec             int                    `json:"lagSec"`
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
	lastIncremental  *IncrementalPoint
	backfillActive   bool
	currentBackfill  *BackfillProgressPoint

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

// Pipeline 当前接线图状态
func (mon *Monitor) Pipeline() PipelineSnapshot {
	now := time.Now()
	snap := PipelineSnapshot{
		At:    now.UnixMilli(),
		AtStr: common.FormatMs(now.UnixMilli()),
	}
	if mon.mgr != nil {
		snap.EsReady = mon.mgr.ES != nil
		snap.MysqlReady = mon.mgr.MySQL != nil
		if mon.mgr.cfg != nil {
			snap.IntervalSec = mon.mgr.cfg.Sync.Interval
			snap.LagSec = mon.mgr.cfg.Sync.LagSeconds
		}
		if mon.mgr.Syncer != nil {
			snap.IncrementalRunning = mon.mgr.Syncer.incrRunning
		}
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
func (mon *Monitor) BeginBackfill(rangeStart, rangeEnd string, totalWindows int) {
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
		mon.currentBackfill = &pt
		mon.backfillProgress = append(mon.backfillProgress, pt)
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
		pending := totalWindows
		var pct float64
		if pending > 0 {
			pct = float64(completed+failed) / float64(pending) * 100
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
		mon.currentBackfill = &pt
		mon.backfillProgress = append(mon.backfillProgress, pt)
		mon.mu.Unlock()
		mon.prune(now)
		mon.broadcast(SSEMessage{Event: "backfill", Data: pt})
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
	})
}

// EndBackfill 补全结束
func (mon *Monitor) EndBackfill() {
	if mon == nil {
		return
	}
	mon.enqueue(func() {
		mon.mu.Lock()
		mon.backfillActive = false
		mon.mu.Unlock()
		mon.broadcast(SSEMessage{Event: "pipeline", Data: mon.Pipeline()})
	})
}
