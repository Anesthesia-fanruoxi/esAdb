package store

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// trackedQueue 生产者窗口队列，跟踪入队/出队/backlog
type trackedQueue struct {
	ch       chan timeWindow
	capacity int
	enqueued int64
	dequeued int64
}

func newTrackedQueue(capacity int) *trackedQueue {
	if capacity < 1 {
		capacity = 1
	}
	return &trackedQueue{
		ch:       make(chan timeWindow, capacity),
		capacity: capacity,
	}
}

func (q *trackedQueue) send(w timeWindow) {
	q.ch <- w
	atomic.AddInt64(&q.enqueued, 1)
}

func (q *trackedQueue) recv() (timeWindow, bool) {
	w, ok := <-q.ch
	if ok {
		atomic.AddInt64(&q.dequeued, 1)
	}
	return w, ok
}

func (q *trackedQueue) close() {
	close(q.ch)
}

func (q *trackedQueue) backlog() int64 {
	b := atomic.LoadInt64(&q.enqueued) - atomic.LoadInt64(&q.dequeued)
	if b < 0 {
		return 0
	}
	return b
}

func (q *trackedQueue) enqueuedTotal() int64 { return atomic.LoadInt64(&q.enqueued) }
func (q *trackedQueue) dequeuedTotal() int64 { return atomic.LoadInt64(&q.dequeued) }

// rangeJobMeta 当前历史补全任务元信息
type rangeJobMeta struct {
	Start    string
	End      string
	Interval int
	Workers  int
}

// refreshProgress 根据运行时状态重建内存 progress map（供 SSE / 查询）
func (s *Syncer) refreshProgress() map[string]interface{} {
	if s == nil {
		return map[string]interface{}{"enabled": false}
	}

	interval := s.cfg.Sync.Interval
	s.mu.Lock()
	lastWindow := s.lastWindow
	incrRunning := s.incrRunning
	histRunning := s.histRunning
	s.mu.Unlock()

	s.seenMu.Lock()
	seenCount := len(s.seen)
	s.seenMu.Unlock()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	esReady := s.mgr != nil && s.mgr.ES != nil
	mysqlReady := s.mgr != nil && s.mgr.MySQL != nil

	service := map[string]interface{}{
		"enabled":     esReady || mysqlReady,
		"ready":       s.cfg != nil && s.cfg.Ready,
		"incremental": incrRunning,
		"historical":  histRunning,
		"startedAt":   formatTime(s.startedAt),
		"interval":    interval,
		"lastWindow":  lastWindow,
		"workers":     runtime.NumCPU(),
		"esReady":     esReady,
		"mysqlReady":  mysqlReady,
	}

	memory := map[string]interface{}{
		"seenWindows":  seenCount,
		"heapAllocMB":  round2(float64(ms.HeapAlloc) / 1024 / 1024),
		"heapSysMB":    round2(float64(ms.HeapSys) / 1024 / 1024),
		"heapInuseMB":  round2(float64(ms.HeapInuse) / 1024 / 1024),
		"numGoroutine": runtime.NumGoroutine(),
		"numGC":        ms.NumGC,
	}

	total := atomic.LoadInt64(&s.histTotal)
	done := atomic.LoadInt64(&s.histCompleted)
	failed := atomic.LoadInt64(&s.histFailed)
	skipped := atomic.LoadInt64(&s.histSkipped)
	finished := done + failed + skipped
	remaining := total - finished
	if remaining < 0 {
		remaining = 0
	}
	pending := total - skipped
	var pct float64
	if pending > 0 {
		pct = float64(done+failed) / float64(pending) * 100
	} else if total > 0 {
		pct = 100
	}
	if pct > 100 {
		pct = 100
	}

	progress := map[string]interface{}{
		"total":     total,
		"done":      done,
		"failed":    failed,
		"skipped":   skipped,
		"remaining": remaining,
		"percent":   fmt.Sprintf("%.1f", pct),
	}

	s.rangeMu.RLock()
	q := s.activeQueue
	producerDone := s.producerDone
	meta := s.rangeMeta
	s.rangeMu.RUnlock()

	queue := map[string]interface{}{
		"active":       histRunning && q != nil,
		"capacity":     0,
		"backlog":      int64(0),
		"enqueued":     int64(0),
		"dequeued":     int64(0),
		"inFlight":     seenCount,
		"producerDone": producerDone,
	}
	if q != nil {
		queue["capacity"] = q.capacity
		queue["backlog"] = q.backlog()
		queue["enqueued"] = q.enqueuedTotal()
		queue["dequeued"] = q.dequeuedTotal()
	}
	if meta.Start != "" {
		progress["rangeStart"] = meta.Start
		progress["rangeEnd"] = meta.End
		progress["rangeWorkers"] = meta.Workers
		progress["rangeInterval"] = meta.Interval
	}

	snap := map[string]interface{}{
		"timestamp": time.Now().Format("2006-01-02 15:04:05"),
		"service":   service,
		"queue":     queue,
		"progress":  progress,
	}
	memory["progressMapKB"] = round2(float64(approxMapSize(snap)+200) / 1024) // +memory 字段估算
	snap["memory"] = memory

	s.progressMu.Lock()
	s.progress = snap
	s.progressMu.Unlock()

	return snap
}

// Snapshot 返回当前进度快照（只读副本）
func (s *Syncer) Snapshot() map[string]interface{} {
	if s == nil {
		return map[string]interface{}{"enabled": false}
	}
	snap := s.refreshProgress()
	s.progressMu.RLock()
	defer s.progressMu.RUnlock()
	return cloneMap(snap)
}

func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}

func approxMapSize(m map[string]interface{}) int {
	b, err := json.Marshal(m)
	if err != nil {
		return 0
	}
	return len(b)
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch t := v.(type) {
		case map[string]interface{}:
			dst[k] = cloneMap(t)
		default:
			dst[k] = v
		}
	}
	return dst
}
