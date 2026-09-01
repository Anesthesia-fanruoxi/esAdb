package store

import (
	"context"
	"time"

	"esAdb/common"
)

// 自动对比-修复任务：每小时整点后 delay 秒触发一次
// 流程：对比上一整点小时 → diff≠0 则 2 层下钻（5分钟→10秒）→ 最深层异常窗口补全一次 → 补后验证
var (
	// autoDrillLadderMs 自动下钻 2 层粒度：5 分钟（12 窗）→ 10 秒（每 5 分钟 30 窗）
	autoDrillLadderMs = []int64{300 * 1000, 10 * 1000}
)

// autoFixMaxWindows 单次自动补全窗口数上限（与手动窗口补全接口一致）
const autoFixMaxWindows = 4000

// StartAutoCompare 启动每小时自动对比任务（goroutine 常驻，随进程退出）
func (m *Manager) StartAutoCompare() {
	if m == nil || m.cfg == nil {
		return
	}
	c := m.cfg.AutoCompare
	if !c.Enabled {
		common.Info("[auto-compare] 未启用（auto_compare.enabled=false）")
		return
	}
	if m.ES == nil || m.MySQL == nil {
		common.Warn("[auto-compare] ES 或 MySQL 未就绪，跳过启动")
		return
	}
	go func() {
		for {
			next := nextHourFireTime(time.Now(), time.Duration(c.DelaySeconds)*time.Second)
			common.Info("[auto-compare] 下次触发 %s", next.Format("2006-01-02 15:04:05"))
			time.Sleep(time.Until(next))
			m.autoCompareOnce()
		}
	}()
	common.Info("[auto-compare] 已启动 delay=%ds workers=%d 粒度=5分钟→10秒", c.DelaySeconds, c.Workers)
}

// nextHourFireTime 下一个触发时刻：整点 + delay（已过本整点触发点则取下一整点）
func nextHourFireTime(now time.Time, delay time.Duration) time.Time {
	h := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, now.Location()).Add(delay)
	if !h.After(now) {
		h = h.Add(time.Hour)
	}
	return h
}

// autoCompareOnce 单次执行：对比 → 下钻 → 补全一次 → 补后验证（不循环重试）
func (m *Manager) autoCompareOnce() {
	win := common.PrevClockHourWindow(time.Now())
	result, err := m.CompareRange(win)
	if err != nil {
		common.Error("[auto-compare] [%s, %s) 对比失败: %v", win.Start, win.End, err)
		return
	}
	common.Info("[auto-compare] [%s, %s) ES=%d ADB=%d diff=%d",
		win.Start, win.End, result.ES.Count, result.MySQL.Count, result.Diff)
	if result.Diff == 0 {
		return
	}

	workers := m.cfg.AutoCompare.Workers
	if workers <= 0 {
		workers = 8
	}
	if !m.DrillMu.TryLock() {
		common.Warn("[auto-compare] 手动下钻进行中，本小时跳过自动分析（diff=%d）", result.Diff)
		return
	}
	levels, err := m.DrilldownLevels(context.Background(), win, autoDrillLadderMs, workers, nil, nil)
	m.DrillMu.Unlock()
	if err != nil {
		common.Error("[auto-compare] 下钻失败: %v", err)
		return
	}

	// 取最深层异常窗口：父窗口差异必然由子窗口体现，最深层为最小充分集合
	var wins []common.TimeRangeMs
	for i := len(levels) - 1; i >= 0; i-- {
		if len(levels[i].Windows) > 0 {
			for _, r := range levels[i].Windows {
				wins = append(wins, r.Range)
			}
			break
		}
	}
	if len(wins) == 0 {
		common.Warn("[auto-compare] diff=%d 但未定位到异常窗口，跳过补全", result.Diff)
		return
	}
	if len(wins) > autoFixMaxWindows {
		common.Warn("[auto-compare] 异常窗口 %d 超上限 %d，截断处理", len(wins), autoFixMaxWindows)
		wins = wins[:autoFixMaxWindows]
	}

	// 与手动补全互斥；补全一次，不做断点/重试。手动任务（如整月补全）可能耗时数小时，
	// 阻塞等待会拖垮每小时节拍，拿不到锁直接跳过本小时。
	// 走窗口补全逻辑（与手动 /sync/backfill/windows 一致）：逐窗 SyncWindow
	if !m.BackfillMu.TryLock() {
		common.Warn("[auto-compare] 手动补全进行中，本小时跳过自动补全（diff=%d）", result.Diff)
		return
	}
	summary := m.BackfillWindows(wins, win.Start, win.End)
	m.BackfillMu.Unlock()
	hour := time.UnixMilli(win.StartMs).Hour()
	if summary.Failed == 0 {
		common.Info("[auto-compare] 当前时段%d点，已补全%d窗口数据，共%d条（补前差异%d）",
			hour, len(wins), summary.TotalWritten, result.Diff)
	} else {
		common.Warn("[auto-compare] 当前时段%d点，已补全%d窗口数据，共%d条，失败%d窗口",
			hour, len(wins), summary.TotalWritten, summary.Failed)
		for _, f := range summary.Windows {
			common.Warn("[auto-compare] 失败窗口 [%s, %s): %s", f.Window.Start, f.Window.End, f.Error)
		}
	}

	// 补后验证：仅记日志，差异若为 ES 重复文档等口径差不会消除，不做二次补全
	after, err := m.CompareRange(win)
	if err != nil {
		common.Warn("[auto-compare] 补后验证对比失败: %v", err)
		return
	}
	common.Info("[auto-compare] 补后验证 [%s, %s) diff=%d（补前 %d）",
		win.Start, win.End, after.Diff, result.Diff)
}
