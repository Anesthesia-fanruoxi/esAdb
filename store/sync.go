package store

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"esAdb/common"
)

// WindowSyncResult 单窗口同步结果
type WindowSyncResult struct {
	Window  common.TimeRangeMs `json:"window"`
	Hits    int                `json:"hits"`
	Written int                `json:"written"`
	Error   string             `json:"error,omitempty"`
}

// BackfillSummary 补全汇总
type BackfillSummary struct {
	TotalWindows int                `json:"totalWindows"`
	Workers      int                `json:"workers"`
	TotalHits    int                `json:"totalHits"`
	TotalWritten int                `json:"totalWritten"`
	Failed       int                `json:"failed"`
	Windows      []WindowSyncResult `json:"windows"`
}

// SyncWindow 单窗口：查 ES → 写 ADB
func (m *Manager) SyncWindow(win common.TimeRangeMs) (hits, written int, err error) {
	if m == nil {
		return 0, 0, fmt.Errorf("Manager 未初始化")
	}
	if m.ES == nil {
		return 0, 0, fmt.Errorf("ES 未就绪")
	}
	if m.MySQL == nil {
		return 0, 0, fmt.Errorf("MySQL 未就绪")
	}

	maxSize := m.cfg.Sync.MaxSize
	if maxSize <= 0 {
		maxSize = 1000
	}

	records, err := m.ES.SearchByRangeMs(win.StartMs, win.EndMs, maxSize, true)
	if err != nil {
		return 0, 0, err
	}
	hits = len(records)
	if hits == maxSize {
		common.Warn("窗口 [%s, %s) 命中数达到 max_size=%d，可能有遗漏",
			win.Start, win.End, maxSize)
	}

	logs := BuildFromESRecords(records)
	written, _, err = m.MySQL.BatchInsertIgnore(logs)
	if err != nil {
		return hits, 0, err
	}

	common.Debug("窗口 [%s, %s) ES=%d 写入=%d", win.Start, win.End, hits, written)
	return hits, written, nil
}

// SyncWindowWithRetry 带重试的单窗口同步
func (m *Manager) SyncWindowWithRetry(win common.TimeRangeMs) error {
	_, _, err := m.SyncWindowWithRetryResult(win)
	return err
}

// SyncWindowWithRetryResult 带重试，返回最后一次尝试的 hits/written
func (m *Manager) SyncWindowWithRetryResult(win common.TimeRangeMs) (hits, written int, err error) {
	if m == nil || m.cfg == nil {
		return 0, 0, fmt.Errorf("Manager 未初始化")
	}
	maxRetry := m.cfg.Sync.MaxRetry
	if maxRetry < 0 {
		maxRetry = 0
	}
	delay := time.Duration(m.cfg.Sync.RetryDelay) * time.Second
	if delay <= 0 {
		delay = time.Second
	}
	maxDelay := time.Duration(m.cfg.Sync.RetryDelayMax) * time.Second
	if maxDelay <= 0 {
		maxDelay = 10 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		hits, written, err = m.SyncWindow(win)
		if err == nil {
			return hits, written, nil
		}
		lastErr = err
		if attempt >= maxRetry {
			break
		}
		common.Warn("窗口 [%s, %s) 同步失败，%v 后重试 (%d/%d): %v",
			win.Start, win.End, delay, attempt+1, maxRetry, err)
		time.Sleep(delay)
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
	return hits, written, lastErr
}

// BackfillWindows 并行补全多个窗口
func (m *Manager) BackfillWindows(windows []common.TimeRangeMs, rangeStart, rangeEnd string) *BackfillSummary {
	summary := &BackfillSummary{TotalWindows: len(windows)}
	if len(windows) == 0 {
		return summary
	}

	if m.Monitor != nil {
		first := windows[0]
		last := windows[len(windows)-1]
		m.Monitor.BeginBackfill(rangeStart, rangeEnd, len(windows), first, last)
		defer m.Monitor.EndBackfill()
	}

	workers := m.cfg.Sync.BackfillWorkers
	if workers <= 0 {
		workers = 2
	}
	if n := runtime.NumCPU(); workers > n && n > 0 {
		workers = n
	}
	summary.Workers = workers
	pause := time.Duration(m.cfg.Sync.BackfillPauseMs) * time.Millisecond
	common.Info("[backfill] workers=%d pause=%v totalWindows=%d", workers, pause, len(windows))

	ch := make(chan common.TimeRangeMs, len(windows))
	for _, w := range windows {
		ch <- w
	}
	close(ch)

	var (
		results       []WindowSyncResult
		mu            sync.Mutex
		wg            sync.WaitGroup
		completed     int
		failed        int
		totalHits     int
		totalWritten  int
		progressCount int
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for win := range ch {
				res := WindowSyncResult{Window: win}
				hits, written, err := m.SyncWindow(win)
				res.Hits = hits
				res.Written = written
				if err != nil {
					res.Error = err.Error()
				}

				mu.Lock()
				results = append(results, res)
				if res.Error != "" {
					failed++
				} else {
					completed++
				}
				totalHits += res.Hits
				totalWritten += res.Written
				progressCount++
				c, f, h, w := completed, failed, totalHits, totalWritten
				pc := progressCount
				total := len(windows)
				shouldUpdate := pc%5 == 0 || c+f == total
				mu.Unlock()

				if m.Monitor != nil {
					m.Monitor.RecordBackfillWindow(res)
					if shouldUpdate {
						m.Monitor.UpdateBackfillProgress(c, f, h, w, total, rangeStart, rangeEnd)
					}
				}

				// 窗口间隔，降低对 ES/ADB/本机的持续压力
				if pause > 0 {
					time.Sleep(pause)
				}
			}
		}()
	}
	wg.Wait()

	if m.Monitor != nil {
		m.Monitor.UpdateBackfillProgress(completed, failed, totalHits, totalWritten, len(windows), rangeStart, rangeEnd)
	}

	summary.Windows = sortResultsByWindow(results, windows)
	for _, r := range summary.Windows {
		summary.TotalHits += r.Hits
		summary.TotalWritten += r.Written
		if r.Error != "" {
			summary.Failed++
		}
	}
	return summary
}

func sortResultsByWindow(results []WindowSyncResult, order []common.TimeRangeMs) []WindowSyncResult {
	byStart := make(map[int64]WindowSyncResult, len(results))
	for _, r := range results {
		byStart[r.Window.StartMs] = r
	}
	out := make([]WindowSyncResult, 0, len(order))
	for _, w := range order {
		if r, ok := byStart[w.StartMs]; ok {
			out = append(out, r)
		}
	}
	return out
}

// CompareRange 对比 ES 与 ADB 在指定时间范围内的条数
func (m *Manager) CompareRange(win common.TimeRangeMs) (*common.CompareResult, error) {
	if m == nil || m.ES == nil || m.MySQL == nil {
		return nil, fmt.Errorf("ES 或 MySQL 未就绪")
	}
	esCount, err := m.ES.CountByRangeMs(win.StartMs, win.EndMs)
	if err != nil {
		return nil, fmt.Errorf("ES 统计失败: %w", err)
	}
	mysqlCount, err := m.MySQL.CountByEsTimestamp(win.StartMs, win.EndMs)
	if err != nil {
		return nil, fmt.Errorf("ADB 统计失败: %w", err)
	}
	dateField := m.cfg.ES.DateField
	if dateField == "" {
		dateField = "@timestamp"
	}
	diff := esCount - mysqlCount
	return &common.CompareResult{
		Range: win,
		ES: common.CompareSide{
			StartMs: win.StartMs, EndMs: win.EndMs,
			Start: win.Start, End: win.End,
			Field: dateField, Count: esCount,
		},
		MySQL: common.CompareSide{
			StartMs: win.StartMs, EndMs: win.EndMs,
			Start: win.Start, End: win.End,
			Field: "es_timestamp", Count: mysqlCount,
		},
		Diff:  diff,
		Match: diff == 0,
	}, nil
}
