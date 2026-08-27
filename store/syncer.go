package store

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"esAdb/common"
	"esAdb/config"
)

type timeWindow struct {
	Start time.Time
	End   time.Time
	key   string
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

	// seen 仅标记进行中窗口，完成后释放，避免长期占用内存
	seenMu sync.Mutex
	seen   map[string]struct{}

	// progress 内存 map，refreshProgress 重建，供 SSE 读取
	progressMu sync.RWMutex
	progress   map[string]interface{}

	// 历史补全进度计数
	histTotal     int64
	histCompleted int64
	histFailed    int64
	histSkipped   int64

	// 当前补全任务队列与元信息
	rangeMu      sync.RWMutex
	activeQueue  *trackedQueue
	producerDone bool
	rangeMeta    rangeJobMeta
}

func NewSyncer(cfg *config.Config, mgr *Manager) *Syncer {
	return &Syncer{
		cfg:      cfg,
		mgr:      mgr,
		seen:     make(map[string]struct{}),
		progress: make(map[string]interface{}),
	}
}

// tryClaimWindow 尝试占用窗口；已占用则返回 false
func (s *Syncer) tryClaimWindow(key string) bool {
	s.seenMu.Lock()
	defer s.seenMu.Unlock()
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}
	return true
}

// releaseWindow 窗口处理结束（成功或放弃）后释放占用
func (s *Syncer) releaseWindow(key string) {
	s.seenMu.Lock()
	delete(s.seen, key)
	s.seenMu.Unlock()
}

// StartIncremental 启动即查上一窗，之后每 interval 秒固定执行一次
func (s *Syncer) StartIncremental(ctx context.Context) {
	if s.mgr == nil || s.mgr.ES == nil || s.mgr.MySQL == nil {
		common.Warn("增量同步未启动：ES 或 MySQL 未就绪")
		return
	}

	interval := s.cfg.Sync.Interval
	now := time.Now()
	s.startedAt = now

	common.Info("增量同步已启动 startedAt=%s interval=%ds（启动即查上一窗，之后每 interval 秒）",
		s.startedAt.Format("2006-01-02 15:04:05"), interval)

	ctx, s.cancelIncr = context.WithCancel(ctx)
	s.incrRunning = true

	go func() {
		defer func() { s.incrRunning = false }()
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()

		s.refreshProgress()
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
		s.mu.Lock()
		s.lastWindow = key
		s.mu.Unlock()
		return
	}

	if err := s.syncWindow(start, end, "incremental"); err != nil {
		common.Error("增量同步失败 [%s,%s): %v",
			start.Format("15:04:05"), end.Format("15:04:05"), err)
		s.releaseWindow(key)
		return
	}

	s.releaseWindow(key)
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
		ref := s.startedAt
		if ref.IsZero() {
			ref = time.Now()
		}
		end = common.AlignFloor(ref, interval)
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
	totalWindows := common.CountWindows(start, end, interval)

	info := map[string]interface{}{
		"mode":         "range",
		"start":        start.Format("2006-01-02 15:04:05"),
		"end":          end.Format("2006-01-02 15:04:05"),
		"interval":     interval,
		"workers":      workers,
		"totalWindows": totalWindows,
		"status":       "accepted",
		"startedAt":    formatTime(s.startedAt),
	}

	go s.runRangeWorkers(start, end, interval, workers, totalWindows)

	return info, nil
}

func (s *Syncer) resetHistProgress(total int64) {
	atomic.StoreInt64(&s.histTotal, total)
	atomic.StoreInt64(&s.histCompleted, 0)
	atomic.StoreInt64(&s.histFailed, 0)
	atomic.StoreInt64(&s.histSkipped, 0)
}

func (s *Syncer) clearHistProgress() {
	atomic.StoreInt64(&s.histTotal, 0)
	atomic.StoreInt64(&s.histCompleted, 0)
	atomic.StoreInt64(&s.histFailed, 0)
	atomic.StoreInt64(&s.histSkipped, 0)
}

func (s *Syncer) logRangeProgress(final bool) {
	total := atomic.LoadInt64(&s.histTotal)
	if total <= 0 {
		return
	}
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
	} else {
		pct = 100
	}
	if pct > 100 {
		pct = 100
	}
	tag := "历史补全进度"
	if final {
		tag = "历史补全完成"
	}
	common.Info("%s total=%d done=%d failed=%d skipped=%d remaining=%d progress=%.1f%%",
		tag, total, done, failed, skipped, remaining, pct)
}

func (s *Syncer) runRangeWorkers(start, end time.Time, interval, workers int, totalWindows int64) {
	defer func() {
		s.logRangeProgress(true)
		s.clearHistProgress()
		s.mu.Lock()
		s.histRunning = false
		s.mu.Unlock()
	}()

	s.resetHistProgress(totalWindows)
	s.refreshProgress()

	s.rangeMu.Lock()
	s.activeQueue = newTrackedQueue(workers * 2)
	s.producerDone = false
	s.rangeMeta = rangeJobMeta{
		Start:    start.Format("2006-01-02 15:04:05"),
		End:      end.Format("2006-01-02 15:04:05"),
		Interval: interval,
		Workers:  workers,
	}
	tq := s.activeQueue
	s.rangeMu.Unlock()

	common.Info("历史同步开始 [%s → %s] interval=%ds workers=%d totalWindows=%d",
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		interval, workers, totalWindows)

	var wg sync.WaitGroup
	progressDone := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				s.refreshProgress()
				s.logRangeProgress(false)
			}
		}
	}()

	// 生产者：按窗入队，已 claim 过的计入 skipped
	go func() {
		defer func() {
			s.rangeMu.Lock()
			s.producerDone = true
			s.rangeMu.Unlock()
			tq.close()
		}()
		for cur := start; cur.Before(end); cur = cur.Add(time.Duration(interval) * time.Second) {
			next := cur.Add(time.Duration(interval) * time.Second)
			if next.After(end) {
				next = end
			}
			key := common.WindowKey(cur, next)
			if !s.tryClaimWindow(key) {
				atomic.AddInt64(&s.histSkipped, 1)
				continue
			}
			tq.send(timeWindow{Start: cur, End: next, key: key})
		}
	}()

	var totalIns, totalSkip int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				w, ok := tq.recv()
				if !ok {
					return
				}
				ins, skip, err := s.syncWindowCount(w.Start, w.End, fmt.Sprintf("range-w%d", id))
				key := w.key
				if err != nil {
					common.Error("历史窗口失败 worker=%d [%s,%s): %v",
						id, w.Start.Format("15:04:05"), w.End.Format("15:04:05"), err)
					s.releaseWindow(key)
					if s.retryWindow(w.Start, w.End) {
						atomic.AddInt64(&s.histCompleted, 1)
					} else {
						atomic.AddInt64(&s.histFailed, 1)
					}
					continue
				}
				atomic.AddInt64(&s.histCompleted, 1)
				s.releaseWindow(key)
				atomic.AddInt64(&totalIns, int64(ins))
				atomic.AddInt64(&totalSkip, int64(skip))
			}
		}(i)
	}
	wg.Wait()
	close(progressDone)

	s.rangeMu.Lock()
	s.activeQueue = nil
	s.producerDone = false
	s.rangeMeta = rangeJobMeta{}
	s.rangeMu.Unlock()
	s.refreshProgress()

	common.Info("历史同步汇总 inserted=%d rowsAffected=%d",
		atomic.LoadInt64(&totalIns), atomic.LoadInt64(&totalSkip))
}

func (s *Syncer) retryWindow(start, end time.Time) bool {
	maxRetry := s.cfg.Sync.MaxRetry
	delay := time.Duration(s.cfg.Sync.RetryDelay) * time.Second
	maxDelay := time.Duration(s.cfg.Sync.RetryDelayMax) * time.Second
	key := common.WindowKey(start, end)
	for i := 0; i < maxRetry; i++ {
		time.Sleep(delay)
		if !s.tryClaimWindow(key) {
			return true
		}
		if err := s.syncWindow(start, end, "range-retry"); err == nil {
			s.releaseWindow(key)
			return true
		}
		s.releaseWindow(key)
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
	isRange := strings.HasPrefix(mode, "range")
	records, err := s.mgr.ES.SearchByRange(start, end, size, !isIncr && !isRange)
	if err != nil {
		return 0, 0, err
	}
	logs := BuildFromESRecords(records)
	ins, skip, err := s.mgr.MySQL.BatchInsertIgnore(logs)
	if err != nil {
		return 0, 0, err
	}
	if isIncr {
		common.Info("[incremental] 写入 [%s,%s) fetched=%d written=%d rowsAffected=%d",
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
