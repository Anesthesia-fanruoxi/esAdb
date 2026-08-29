package store

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"esAdb/common"
)

const (
	// DrilldownDayMs 第一级：日窗口（金字塔剪枝，先按日收敛，支持对比整月）
	DrilldownDayMs = int64(24 * 3600 * 1000)
	// DrilldownHourMs 第二级：小时窗口
	DrilldownHourMs = int64(3600 * 1000)
	// DrilldownMinuteMs 第三级：5 分钟块（300s = 30 个 10 秒窗）
	DrilldownMinuteMs = int64(5 * 60 * 1000)
	// DrilldownTenSecMs 第四级：10 秒窗口（最细，用于定位与补全）
	DrilldownTenSecMs = int64(10 * 1000)
	// maxDrilldownWindows 单级窗口数量上限，防范围过大。
	// 需覆盖最细粒度：24h 全异常时 10 秒窗可达 8640 个（异常 5 分钟块 × 30）
	maxDrilldownWindows = 20000
)

// DrilldownLevel 一级下钻产生的异常窗口集合（最终用于补全）
type DrilldownLevel struct {
	Level    int                    `json:"level"`
	LevelMs  int64                  `json:"levelMs"`
	Total    int                    `json:"total"`    // 该级实际查询的窗口总数
	Abnormal int                    `json:"abnormal"` // 异常窗口（diff != 0）数量
	Windows  []common.CompareResult `json:"windows"`  // 异常窗口，按 start 升序
}

// DrillStatus 单个窗口的流式进度状态（前端按层级填充状态格）
type DrillStatus struct {
	Level   int                `json:"level"`
	LevelMs int64              `json:"levelMs"`
	Parent  common.TimeRangeMs `json:"parent"` // 该窗口所属的父窗口（第1级=整体范围）
	Window  common.TimeRangeMs `json:"window"`
	ES      int                `json:"es"`
	ADB     int                `json:"adb"`
	Diff    int                `json:"diff"`
	Match   bool               `json:"match"`
}

// onDrillStatus 每个窗口计算完成即回调一次
type onDrillStatus func(DrillStatus)

// subWin 某个子窗口及其所属父窗口
type subWin struct {
	parent common.TimeRangeMs
	win    common.TimeRangeMs
}

// queryDrilldownLevel 对 parents 中每个区间按 lvMs 切窗，并发查询 ES/ADB 计数。
// 每个子窗口算完即回调 st；每处理完一个子窗口(含失败)即回调 prog(done,total) 上报进度；
// 筛选出 diff != 0 的异常窗口统一返回。
func (m *Manager) queryDrilldownLevel(ctx context.Context, parents []common.TimeRangeMs, lvMs int64, workers int, st onDrillStatus, prog func(done, total int)) (DrilldownLevel, error) {
	if workers <= 0 {
		workers = 8
	}
	var subs []subWin
	total := 0
	for _, p := range parents {
		for _, s := range common.SplitWindowsMs(p.StartMs, p.EndMs, lvMs) {
			subs = append(subs, subWin{parent: p, win: s})
			total++
		}
		if total > maxDrilldownWindows {
			return DrilldownLevel{}, fmt.Errorf("单级窗口数量过大(%d > %d)，请缩小对比范围后再分析", total, maxDrilldownWindows)
		}
	}
	if len(subs) == 0 {
		if prog != nil {
			prog(0, 0)
		}
		common.Info("[drilldown] level(ms=%d) 无窗口可查", lvMs)
		return DrilldownLevel{LevelMs: lvMs, Total: total}, nil
	}
	if prog != nil {
		prog(0, total)
	}
	if workers > len(subs) {
		workers = len(subs)
	}

	common.Info("[drilldown] level(ms=%d) 开始 parents=%d windows=%d workers=%d",
		lvMs, len(parents), total, workers)

	jobs := make(chan int)
	var (
		mu        sync.Mutex
		abnormal  = []common.CompareResult{}
		failCnt   int
		processed int
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				select {
				case <-ctx.Done():
					continue
				default:
				}
				sw := subs[idx]
				r, err := m.CompareRange(sw.win)
				if err != nil {
					mu.Lock()
					failCnt++
					mu.Unlock()
					common.Warn("[drilldown] 窗口对比失败 [%s, %s): %s",
						sw.win.Start, sw.win.End, err.Error())
				} else if r.Diff != 0 {
					// 仅将异常窗口上抛给 SSE，正常窗口直接剪枝，避免无效窗口过多
					if st != nil {
						st(DrillStatus{
							Level: 0, LevelMs: lvMs,
							Parent: sw.parent, Window: sw.win,
							ES: r.ES.Count, ADB: r.MySQL.Count, Diff: r.Diff, Match: r.Match,
						})
					}
					mu.Lock()
					abnormal = append(abnormal, *r)
					mu.Unlock()
				}
				// 每个窗口算完(含失败)都上报一次进度，驱动前端进度条逐步推进
				mu.Lock()
				processed++
				done := processed
				mu.Unlock()
				if prog != nil {
					prog(done, total)
				}
			}
		}()
	}
	for i := range subs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	sort.Slice(abnormal, func(i, j int) bool {
		return abnormal[i].Range.StartMs < abnormal[j].Range.StartMs
	})
	common.Info("[drilldown] level(ms=%d) 完成 total=%d abnormal=%d failed=%d",
		lvMs, total, len(abnormal), failCnt)
	return DrilldownLevel{LevelMs: lvMs, Total: total, Abnormal: len(abnormal), Windows: abnormal}, nil
}

// DrilldownLevels 逐级下钻：levelsMs 为空时默认 [小时, 5分钟, 10秒]，
// 每一级仅在前一级的异常窗口内继续细分；每级每窗口算完即回调 st（需带 currentLevel 序号）。
func (m *Manager) DrilldownLevels(ctx context.Context, win common.TimeRangeMs, levelsMs []int64, workers int, st func(level int, s DrillStatus), onProgress func(level, done, total int)) ([]DrilldownLevel, error) {
	if m == nil || m.ES == nil || m.MySQL == nil {
		return nil, fmt.Errorf("ES 或 MySQL 未就绪")
	}
	if len(levelsMs) == 0 {
		levelsMs = []int64{DrilldownDayMs, DrilldownHourMs, DrilldownMinuteMs, DrilldownTenSecMs}
	}
	// 过滤非正粒度（前端可传 0 表示某一层不下钻，只跑剩余层）
	var pos []int64
	for _, ms := range levelsMs {
		if ms > 0 {
			pos = append(pos, ms)
		}
	}
	levelsMs = pos
	if len(levelsMs) == 0 {
		levelsMs = []int64{DrilldownDayMs, DrilldownHourMs, DrilldownMinuteMs, DrilldownTenSecMs}
	}

	var out []DrilldownLevel
	parents := []common.TimeRangeMs{win}
	for lvNum, ms := range levelsMs {
		lvRes, err := m.queryDrilldownLevel(ctx, parents, ms, workers, func(s DrillStatus) {
			s.Level = lvNum + 1
			if st != nil {
				st(lvNum+1, s)
			}
		}, func(done, total int) {
			if onProgress != nil {
				onProgress(lvNum+1, done, total)
			}
		})
		if err != nil {
			return out, err
		}
		lvRes.Level = lvNum + 1
		out = append(out, lvRes)

		parents = parents[:0]
		for i := range lvRes.Windows {
			parents = append(parents, lvRes.Windows[i].Range)
		}
	}
	return out, nil
}
