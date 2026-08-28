package store

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"esAdb/common"
)

const (
	// DrilldownHourMs 第一级：小时窗口
	DrilldownHourMs = int64(3600 * 1000)
	// DrilldownMinuteMs 第二级：分钟窗口
	DrilldownMinuteMs = int64(60 * 1000)
	// DrilldownTenSecMs 第三级：10 秒窗口
	DrilldownTenSecMs = int64(10 * 1000)
	// maxDrilldownWindows 单级窗口数量上限，防范围过大
	maxDrilldownWindows = 4000
)

// DrilldownLevel 一级下钻产生的异常窗口集合
type DrilldownLevel struct {
	Level    int                    `json:"level"`
	LevelMs  int64                  `json:"levelMs"`
	Total    int                    `json:"total"`    // 该级实际查询的窗口总数
	Abnormal int                    `json:"abnormal"` // 异常窗口（diff != 0）数量
	Windows  []common.CompareResult `json:"windows"`  // 异常窗口，按 start 升序
}

// queryDrilldownLevel 对 parents 中每个区间按 lvMs 切窗，并发查询 ES/ADB 计数，筛选出异常窗口
func (m *Manager) queryDrilldownLevel(ctx context.Context, parents []common.TimeRangeMs, lvMs int64, workers int) (DrilldownLevel, error) {
	if workers <= 0 {
		workers = 8
	}
	var subs []common.TimeRangeMs
	total := 0
	for _, p := range parents {
		for _, s := range common.SplitWindowsMs(p.StartMs, p.EndMs, lvMs) {
			subs = append(subs, s)
			total++
		}
		if total > maxDrilldownWindows {
			return DrilldownLevel{}, fmt.Errorf("单级窗口数量过大(%d > %d)，请缩小对比范围后再分析", total, maxDrilldownWindows)
		}
	}
	if len(subs) == 0 {
		return DrilldownLevel{LevelMs: lvMs, Total: total}, nil
	}
	if workers > len(subs) {
		workers = len(subs)
	}

	jobs := make(chan int)
	var (
		mu       sync.Mutex
		abnormal = []common.CompareResult{}
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					continue
				}
				r, err := m.CompareRange(subs[idx])
				if err != nil {
					continue
				}
				if r.Diff != 0 {
					mu.Lock()
					abnormal = append(abnormal, *r)
					mu.Unlock()
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
	return DrilldownLevel{LevelMs: lvMs, Total: total, Abnormal: len(abnormal), Windows: abnormal}, nil
}

// DrilldownLevels 逐级下钻：levelsMs 为空时默认 [小时, 分钟, 10 秒]，
// 每一级仅在前一级的异常窗口内继续细分
func (m *Manager) DrilldownLevels(ctx context.Context, win common.TimeRangeMs, levelsMs []int64, workers int) ([]DrilldownLevel, error) {
	if m == nil || m.ES == nil || m.MySQL == nil {
		return nil, fmt.Errorf("ES 或 MySQL 未就绪")
	}
	if len(levelsMs) == 0 {
		levelsMs = []int64{DrilldownHourMs, DrilldownMinuteMs, DrilldownTenSecMs}
	}

	var out []DrilldownLevel
	parents := []common.TimeRangeMs{win}
	for lvNum, ms := range levelsMs {
		lvRes, err := m.queryDrilldownLevel(ctx, parents, ms, workers)
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