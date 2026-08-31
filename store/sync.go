package store

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"esAdb/common"
)

// WindowSyncResult 单窗口同步结果
type WindowSyncResult struct {
	Window     common.TimeRangeMs `json:"window"`
	Hits       int                `json:"hits"`
	Written    int                `json:"written"`
	DurationMs int64              `json:"durationMs,omitempty"`
	Error      string             `json:"error,omitempty"`
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
	if dup := hits - len(logs); dup > 0 {
		common.Debug("窗口 [%s, %s) 重复 id 跳过 %d 条", win.Start, win.End, dup)
	}
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

	// 流式供给：小缓冲 + 生产者逐批发，避免一次性把全部窗口驻留内存（整月可到数十万窗口）
	const chanCap = 64
	ch := make(chan common.TimeRangeMs, chanCap)
	go func() {
		defer close(ch)
		for _, w := range windows {
			ch <- w
		}
	}()

	var (
		mu                      sync.Mutex
		wg                      sync.WaitGroup
		completed               int
		failed                  int
		totalHits               int
		totalWritten            int
		progressCount           int
		failedWindows           []WindowSyncResult // 仅保留失败窗口明细，正常运行不驻留全量结果
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for win := range ch {
				res := WindowSyncResult{Window: win}
				winStart := time.Now()
				hits, written, err := m.SyncWindow(win)
				res.DurationMs = time.Since(winStart).Milliseconds()
				res.Hits = hits
				res.Written = written
				if err != nil {
					res.Error = err.Error()
				}

				mu.Lock()
				if res.Error != "" {
					failed++
					failedWindows = append(failedWindows, res)
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

	sort.Slice(failedWindows, func(i, j int) bool {
		return failedWindows[i].Window.StartMs < failedWindows[j].Window.StartMs
	})
	summary.Failed = failed
	summary.TotalHits = totalHits
	summary.TotalWritten = totalWritten
	summary.Windows = failedWindows
	return summary
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
