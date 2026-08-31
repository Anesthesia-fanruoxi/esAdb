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

// 自适应补全窗口
const (
	// SplitThreshold 单窗口命中数达到该值视为可能截断，触发向更小窗口逐级分裂
	SplitThreshold = 10000
	// BackfillBaseIntervalSec 补全初始（最大）窗口粒度：10 分钟
	BackfillBaseIntervalSec = 600
)

// backfillLadderMin 自适应窗口逐级分裂梯级（分钟）：10 → 5 → 2 → 1
var backfillLadderMin = []int64{10, 5, 2, 1}

// nextGranularityMs 返回当前窗口时长的下一级更小粒度（毫秒）；已达最小粒度返回 0 表示不可再分
func nextGranularityMs(winMs int64) int64 {
	for i, g := range backfillLadderMin {
		if winMs >= g*60*1000 {
			if i >= len(backfillLadderMin)-1 {
				return 0
			}
			return backfillLadderMin[i+1] * 60 * 1000
		}
	}
	return 0
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

// SyncWindowAdaptive 自适应单窗口同步：命中数达到阈值则沿 10→5→2→1 分钟逐级分裂。
// 稀疏窗口一次拉取（快），密集窗口自动切小粒度（稳），避免单次查询过大/截断。
func (m *Manager) SyncWindowAdaptive(win common.TimeRangeMs) (hits, written int, err error) {
	size := SplitThreshold
	records, rawHits, err := m.ES.SearchByRangeMsRaw(win.StartMs, win.EndMs, size, true)
	if err != nil {
		return 0, 0, err
	}
	// 未达阈值：整个窗口一次处理
	if rawHits < size {
		return m.applyWindowRecords(win, records)
	}
	// 可再切分：沿梯级分裂为更小窗口，逐级递归
	if subWinMs := nextGranularityMs(win.EndMs - win.StartMs); subWinMs > 0 {
		subWins := common.SplitWindowsMs(win.StartMs, win.EndMs, subWinMs)
		common.Info("窗口 [%s, %s) 命中数=%d 达上限，切分为 %d 个 %d 分钟窗口",
			win.Start, win.End, rawHits, len(subWins), subWinMs/60000)
		var totalHits, totalWritten int
		for _, sw := range subWins {
			h, w, e := m.SyncWindowAdaptive(sw)
			if e != nil {
				return totalHits, totalWritten, e
			}
			totalHits += h
			totalWritten += w
		}
		return totalHits, totalWritten, nil
	}
	// 已达最小粒度 1 分钟仍超上限：直接补前 size 条，超出部分忽略（差异由外部对比分析兜底）
	return m.applyWindowRecords(win, records)
}

// applyWindowRecords 将查询到的 ES 记录写入 ADB，返回命中数与写入数
func (m *Manager) applyWindowRecords(win common.TimeRangeMs, records []ESRecord) (hits, written int, err error) {
	hits = len(records)
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

// BackfillWindows 窗口补全（/sync/backfill/windows）：固定窗口粒度，逐窗 SyncWindow。
func (m *Manager) BackfillWindows(windows []common.TimeRangeMs, rangeStart, rangeEnd string) *BackfillSummary {
	return m.runBackfill(windows, rangeStart, rangeEnd, m.SyncWindow, 0)
}

// BackfillWindowsAdaptive 范围补全（/sync/backfill）：以 10 分钟初始窗口并行，
// 逐窗 SyncWindowAdaptive，命中达阈值自动分裂。独立路径，不影响窗口补全。
// 进度换算单位窗口取自配置 Sync.Interval（默认 10 秒）。
func (m *Manager) BackfillWindowsAdaptive(windows []common.TimeRangeMs, rangeStart, rangeEnd string) *BackfillSummary {
	unitMs := int64(common.DefaultIntervalSec) * 1000
	if m.cfg != nil && m.cfg.Sync.Interval > 0 {
		unitMs = int64(m.cfg.Sync.Interval) * 1000
	}
	return m.runBackfill(windows, rangeStart, rangeEnd, m.SyncWindowAdaptive, unitMs)
}

// syncWindowFunc 单窗口处理函数：查 ES → 写 ADB
type syncWindowFunc func(win common.TimeRangeMs) (hits, written int, err error)

// windowUnits 窗口覆盖的单位窗口数；unitMs<=0 表示不做换算（每窗口计 1）。
// 单位窗口为增量间隔 Sync.Interval（默认 10s）：interval=10 时 10 分钟=60、5 分钟=30、2 分钟=12、1 分钟=6。
func windowUnits(win common.TimeRangeMs, unitMs int64) int {
	if unitMs <= 0 {
		return 1
	}
	n := int((win.EndMs - win.StartMs) / unitMs)
	if n < 1 {
		n = 1
	}
	return n
}

// runBackfill 并行补全公共执行体。
// unitMs>0 时监控进度按单位窗口换算（总数为固定单位数，completed/failed 按窗口覆盖单位数累加，
// 百分比反映时间覆盖，不受 10/5/2/1 分钟分裂粒度影响）；unitMs<=0 时维持字面窗口计数（窗口补全）。
func (m *Manager) runBackfill(windows []common.TimeRangeMs, rangeStart, rangeEnd string, run syncWindowFunc, unitMs int64) *BackfillSummary {
	summary := &BackfillSummary{TotalWindows: len(windows)}
	if len(windows) == 0 {
		return summary
	}

	totalUnits := 0
	for _, w := range windows {
		totalUnits += windowUnits(w, unitMs)
	}

	if m.Monitor != nil {
		first := windows[0]
		last := windows[len(windows)-1]
		m.Monitor.BeginBackfill(rangeStart, rangeEnd, totalUnits, first, last)
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
		completed               int // 字面成功窗口数（用于 summary）
		failed                  int // 字面失败窗口数（用于 summary）
		completedUnits          int // 单位窗口完成进度
		failedUnits             int // 单位窗口失败进度
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
				hits, written, err := run(win)
				res.DurationMs = time.Since(winStart).Milliseconds()
				res.Hits = hits
				res.Written = written
				if err != nil {
					res.Error = err.Error()
				}

				units := windowUnits(win, unitMs)
				mu.Lock()
				if res.Error != "" {
					failed++
					failedUnits += units
					failedWindows = append(failedWindows, res)
				} else {
					completed++
					completedUnits += units
				}
				totalHits += res.Hits
				totalWritten += res.Written
				progressCount++
				c, f, h, w := completed, failed, totalHits, totalWritten
				cu, fu := completedUnits, failedUnits
				pc := progressCount
				total := len(windows)
				shouldUpdate := pc%5 == 0 || c+f == total
				mu.Unlock()

				if m.Monitor != nil {
					m.Monitor.RecordBackfillWindow(res)
					if shouldUpdate {
						m.Monitor.UpdateBackfillProgress(cu, fu, h, w, totalUnits, rangeStart, rangeEnd)
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
		m.Monitor.UpdateBackfillProgress(completedUnits, failedUnits, totalHits, totalWritten, totalUnits, rangeStart, rangeEnd)
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
