package store

import (
	"context"
	"time"

	"esAdb/common"
	"esAdb/config"
)

// Syncer 增量同步调度
type Syncer struct {
	cfg         *config.Config
	mgr         *Manager
	startedAt   time.Time
	incrRunning bool
	cancelIncr  context.CancelFunc
}

func NewSyncer(cfg *config.Config, mgr *Manager) *Syncer {
	return &Syncer{cfg: cfg, mgr: mgr}
}

// StartIncremental 固定每 interval 秒触发一次增量同步
func (s *Syncer) StartIncremental(ctx context.Context) {
	interval := s.cfg.Sync.Interval
	if interval <= 0 {
		interval = common.DefaultIntervalSec
	}
	lag := s.cfg.Sync.LagSeconds
	if lag <= 0 {
		lag = common.DefaultLagSec
	}

	s.startedAt = time.Now()
	common.Info("增量调度已启动 interval=%ds lag=%ds tick=%ds",
		interval, lag, interval)

	ctx, s.cancelIncr = context.WithCancel(ctx)
	s.incrRunning = true

	go func() {
		defer func() { s.incrRunning = false }()
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()

		s.runIncremental()

		for {
			select {
			case <-ctx.Done():
				common.Info("增量调度已停止")
				return
			case <-ticker.C:
				s.runIncremental()
			}
		}
	}()
}

func (s *Syncer) runIncremental() {
	win := common.IncrementalWindow(time.Now(), s.cfg.Sync.Interval, s.cfg.Sync.LagSeconds)
	common.Debug("[incremental] 目标窗口 [%s, %s) startMs=%d endMs=%d",
		win.Start, win.End, win.StartMs, win.EndMs)

	if s.mgr == nil || s.mgr.ES == nil || s.mgr.MySQL == nil {
		common.Warn("[incremental] ES 或 MySQL 未就绪，跳过")
		return
	}

	start := time.Now()
	hits, written, err := s.mgr.SyncWindowWithRetryResult(win)
	if err != nil {
		common.Error("[incremental] 窗口同步失败: %v", err)
	}
	if s.mgr.Monitor != nil {
		s.mgr.Monitor.RecordIncremental(win, hits, written, time.Since(start), err)
	}
}
