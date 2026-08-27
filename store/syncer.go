package store

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"esAdb/common"
	"esAdb/config"
)

type timeWindow struct {
	Start time.Time
	End   time.Time
}

// Syncer 双路径：增量巡检上一窗 + 历史多线程补数
type Syncer struct {
	cfg       *config.Config
	mgr       *Manager
	startedAt time.Time

	mu          sync.Mutex
	histRunning bool
	incrRunning bool
	lastWindow  string // 增量上次已查询的窗口键
	cancelIncr  context.CancelFunc

	// 全局窗口去重：queued/running/done，防增量与历史重复查同一窗
	seenMu sync.Mutex
	seen   map[string]struct{}
}

func NewSyncer(cfg *config.Config, mgr *Manager) *Syncer {
	return &Syncer{
		cfg:  cfg,
		mgr:  mgr,
		seen: make(map[string]struct{}),
	}
}

// tryClaimWindow 尝试占用窗口；已占用/已完成则返回 false
func (s *Syncer) tryClaimWindow(key string) bool {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	return true
}

// releaseWindow 失败时可释放，允许重试
func (s *Syncer) releaseWindow(key string) {
	s.seenMu.Lock()
	delete(s.seen, key)
	s.seenMu.Unlock()
}

// StartIncremental 每 interval/3 秒计算上一已结束窗；与上次相同则跳过
func (s *Syncer) StartIncremental(ctx context.Context) {
	if s.mgr == nil || s.mgr.ES == nil || s.mgr.MySQL == nil {
		common.Warn("增量同步未启动：ES 或 MySQL 未就绪")
		return
	}

	interval := s.cfg.Sync.Interval
	tickSec := common.TickSeconds(interval)
	now := time.Now()
	s.startedAt = now

	common.Info("增量同步已启动 startedAt=%s interval=%ds tick=%ds（查上一已结束窗）",
		s.startedAt.Format("2006-01-02 15:04:05"), interval, tickSec)

	ctx, s.cancelIncr = context.WithCancel(ctx)
	s.incrRunning = true

	go func() {
		defer func() { s.incrRunning = false }()
		ticker := time.NewTicker(time.Duration(tickSec) * time.Second)
		defer ticker.Stop()

		// 启动后先算一次
		s.incrementalOnce(interval)

		for {
			select {
			case <-ctx.Done():
				common.Info("增量同步已停止")
				return
			case <-ticker.C:
				s.incrementalOnce(interval)
			}
		}
	}()
}

func (s *Syncer) incrementalOnce(interval int) {
	start, end := common.PrevWindow(time.Now(), interval)
	key := common.WindowKey(start, end)

	s.mu.Lock()
	if key == s.lastWindow {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	if !s.tryClaimWindow(key) {
		// 历史任务或其他路径已处理
		s.mu.Lock()
		s.lastWindow = key
		s.mu.Unlock()
		return
	}

	if err := s.syncWindow(start, end, "incremental"); err != nil {
		common.Error("增量同步失败 [%s,%s): %v",
			start.Format("15:04:05"), end.Format("15:04:05"), err)
		s.releaseWindow(key) // 允许下次重试
		return
	}

	s.mu.Lock()
	s.lastWindow = key
	s.mu.Unlock()
}

// SyncRange 历史补数：按 CPU 核数开 worker，队列投递窗口并去重
func (s *Syncer) SyncRange(start, end time.Time) (map[string]interface{}, error) {
	if s.mgr == nil || s.mgr.ES == nil || s.mgr.MySQL == nil {
		return nil, fmt.Errorf("ES 或 MySQL 未就绪")
	}

	interval := s.cfg.Sync.Interval
	start = common.AlignFloor(start, interval)
	if end.IsZero() {
		if !s.startedAt.IsZero() {
			end = common.AlignFloor(s.startedAt, interval)
		} else {
			end = common.AlignFloor(time.Now(), interval)
		}
	} else {
		end = common.AlignCeil(end, interval)
	}
	if !start.Before(end) {
		return nil, fmt.Errorf("时间范围无效 start=%s end=%s", start, end)
	}

	s.mu.Lock()
	if s.histRunning {
		s.mu.Unlock()
		return nil, fmt.Errorf("历史同步正在进行中")
	}
	s.histRunning = true
	s.mu.Unlock()

	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}

	info := map[string]interface{}{
		"mode":      "range",
		"start":     start.Format("2006-01-02 15:04:05"),
		"end":       end.Format("2006-01-02 15:04:05"),
		"interval":  interval,
		"workers":   workers,
		"status":    "accepted",
		"startedAt": formatTime(s.startedAt),
	}

	go s.runRangeWorkers(start, end, interval, workers)

	return info, nil
}

func (s *Syncer) runRangeWorkers(start, end time.Time, interval, workers int) {
	defer func() {
		s.mu.Lock()
		s.histRunning = false
		s.mu.Unlock()
	}()

	common.Info("历史同步开始 [%s → %s] interval=%ds workers=%d",
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		interval, workers)

	queue := make(chan timeWindow, workers*2)
	var enqueued, skippedDup int64
	var wg sync.WaitGroup

	// 生产者：按窗入队，已 claim 过的跳过
	go func() {
		defer close(queue)
		for cur := start; cur.Before(end); cur = cur.Add(time.Duration(interval) * time.Second) {
			next := cur.Add(time.Duration(interval) * time.Second)
			if next.After(end) {
				next = end
			}
			key := common.WindowKey(cur, next)
			if !s.tryClaimWindow(key) {
				atomic.AddInt64(&skippedDup, 1)
				continue
			}
			atomic.AddInt64(&enqueued, 1)
			queue <- timeWindow{Start: cur, End: next}
		}
	}()

	var totalIns, totalSkip int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for w := range queue {
				ins, skip, err := s.syncWindowCount(w.Start, w.End, fmt.Sprintf("range-w%d", id))
				if err != nil {
					common.Error("历史窗口失败 worker=%d [%s,%s): %v",
						id, w.Start.Format("15:04:05"), w.End.Format("15:04:05"), err)
					key := common.WindowKey(w.Start, w.End)
					s.releaseWindow(key)
					if s.retryWindow(w.Start, w.End) {
						// 重试成功后重新占用
						s.tryClaimWindow(key)
					}
					continue
				}
				atomic.AddInt64(&totalIns, int64(ins))
				atomic.AddInt64(&totalSkip, int64(skip))
			}
		}(i)
	}
	wg.Wait()

	common.Info("历史同步完成 enqueued=%d dupSkipped=%d inserted=%d skipped=%d",
		atomic.LoadInt64(&enqueued), atomic.LoadInt64(&skippedDup),
		atomic.LoadInt64(&totalIns), atomic.LoadInt64(&totalSkip))
}

func (s *Syncer) retryWindow(start, end time.Time) bool {
	maxRetry := s.cfg.Sync.MaxRetry
	delay := time.Duration(s.cfg.Sync.RetryDelay) * time.Second
	maxDelay := time.Duration(s.cfg.Sync.RetryDelayMax) * time.Second
	for i := 0; i < maxRetry; i++ {
		time.Sleep(delay)
		if err := s.syncWindow(start, end, "range-retry"); err == nil {
			return true
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return false
}

func (s *Syncer) syncWindow(start, end time.Time, mode string) error {
	_, _, err := s.syncWindowCount(start, end, mode)
	return err
}

func (s *Syncer) syncWindowCount(start, end time.Time, mode string) (inserted, skipped int, err error) {
	size := s.cfg.Sync.MaxSize
	isIncr := mode == "incremental"
	records, err := s.mgr.ES.SearchByRange(start, end, size, !isIncr)
	if err != nil {
		return 0, 0, err
	}
	logs := BuildFromESRecords(records)
	ins, skip, err := s.mgr.MySQL.BatchInsertIgnore(logs)
	if err != nil {
		return 0, 0, err
	}
	// 增量只打印一次写入汇总（ADB REPLACE INTO：ins=条数，skip=RowsAffected 参考）
	if isIncr {
		common.Info("[incremental] 写入 [%s,%s) fetched=%d written=%d rowsAffected=%d",
			start.Format("2006-01-02 15:04:05"),
			end.Format("2006-01-02 15:04:05"),
			len(records), ins, skip)
	} else {
		common.Info("[%s] 窗口 [%s,%s) fetched=%d written=%d rowsAffected=%d",
			mode,
			start.Format("2006-01-02 15:04:05"),
			end.Format("2006-01-02 15:04:05"),
			len(records), ins, skip)
	}
	return ins, skip, nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02 15:04:05")
}

// Status 当前同步状态
func (s *Syncer) Status() map[string]interface{} {
	if s == nil {
		return map[string]interface{}{"enabled": false}
	}
	interval := s.cfg.Sync.Interval
	s.mu.Lock()
	last := s.lastWindow
	s.mu.Unlock()
	return map[string]interface{}{
		"enabled":     true,
		"startedAt":   formatTime(s.startedAt),
		"interval":    interval,
		"tick":        common.TickSeconds(interval),
		"lastWindow":  last,
		"workers":     runtime.NumCPU(),
		"incremental": s.incrRunning,
		"historical":  s.histRunning,
	}
}
