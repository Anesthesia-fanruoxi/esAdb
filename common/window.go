package common

import (
	"fmt"
	"time"
)

const (
	// DefaultIntervalSec 默认窗口 10 秒
	DefaultIntervalSec = 10
	// DefaultLagSec ES 写入延迟补偿 60 秒
	DefaultLagSec = 60
)

// TimeRangeMs 毫秒时间范围 [StartMs, EndMs)
type TimeRangeMs struct {
	StartMs int64  `json:"startMs"`
	EndMs   int64  `json:"endMs"`
	Start   string `json:"start"` // 格式化时间（含毫秒）
	End     string `json:"end"`
}

// NewTimeRangeMs 构造带格式化字段的时间范围
func NewTimeRangeMs(startMs, endMs int64) TimeRangeMs {
	return TimeRangeMs{
		StartMs: startMs,
		EndMs:   endMs,
		Start:   FormatMs(startMs),
		End:     FormatMs(endMs),
	}
}

// FormatMs 毫秒时间戳 → 可读字符串
func FormatMs(ms int64) string {
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05.000")
}

// ParseTimeInput 解析接口入参：支持毫秒整数或 "2006-01-02 15:04:05" / 带毫秒
func ParseTimeInput(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("时间为空")
	}
	// 纯数字视为毫秒时间戳
	allDigit := true
	for _, c := range s {
		if c < '0' || c > '9' {
			allDigit = false
			break
		}
	}
	if allDigit {
		var ms int64
		if _, err := fmt.Sscanf(s, "%d", &ms); err != nil {
			return 0, err
		}
		return ms, nil
	}
	layouts := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t.UnixMilli(), nil
		}
	}
	return 0, fmt.Errorf("无法解析时间: %s", s)
}

// IntervalMs 窗口间隔（秒 → 毫秒）
func IntervalMs(intervalSec int) int64 {
	if intervalSec <= 0 {
		intervalSec = DefaultIntervalSec
	}
	return int64(intervalSec) * 1000
}

// LagMs 延迟补偿（秒 → 毫秒）
func LagMs(lagSec int) int64 {
	if lagSec <= 0 {
		lagSec = DefaultLagSec
	}
	return int64(lagSec) * 1000
}

// AlignFloorMs 从 0 毫秒起向下对齐：aligned = ms - ms%intervalMs
func AlignFloorMs(ms, intervalMs int64) int64 {
	if intervalMs <= 0 {
		intervalMs = IntervalMs(DefaultIntervalSec)
	}
	return ms - (ms % intervalMs)
}

// AlignCeilMs 向上对齐；已在边界则返回自身
func AlignCeilMs(ms, intervalMs int64) int64 {
	floor := AlignFloorMs(ms, intervalMs)
	if floor == ms {
		return ms
	}
	return floor + intervalMs
}

// PrevWindowMs 参考时刻的上一已结束窗口 [end-intervalMs, endMs)
func PrevWindowMs(refMs, intervalMs int64) (startMs, endMs int64) {
	endMs = AlignFloorMs(refMs, intervalMs)
	startMs = endMs - intervalMs
	return startMs, endMs
}

// IncrementalWindowMs 增量查询窗口：以 (now - lag) 为基准的前一个 interval 窗口
func IncrementalWindowMs(nowMs, intervalMs, lagMs int64) TimeRangeMs {
	refMs := nowMs - lagMs
	s, e := PrevWindowMs(refMs, intervalMs)
	return NewTimeRangeMs(s, e)
}

// IncrementalWindow 增量窗口（便捷方法）
func IncrementalWindow(now time.Time, intervalSec, lagSec int) TimeRangeMs {
	return IncrementalWindowMs(now.UnixMilli(), IntervalMs(intervalSec), LagMs(lagSec))
}

// SplitWindowsMs 将 [rangeStartMs, rangeEndMs) 切分为多个 interval 窗口
func SplitWindowsMs(rangeStartMs, rangeEndMs, intervalMs int64) []TimeRangeMs {
	if rangeEndMs <= rangeStartMs {
		return nil
	}
	startMs := AlignFloorMs(rangeStartMs, intervalMs)
	var out []TimeRangeMs
	for cur := startMs; cur < rangeEndMs; cur += intervalMs {
		nxt := cur + intervalMs
		if nxt > rangeEndMs {
			nxt = rangeEndMs
		}
		out = append(out, NewTimeRangeMs(cur, nxt))
	}
	return out
}

// TimePointMs 单个时间点（毫秒）
type TimePointMs struct {
	Ms   int64  `json:"ms"`
	Time string `json:"time"`
}

// NewTimePointMs 构造时间点
func NewTimePointMs(ms int64) TimePointMs {
	return TimePointMs{Ms: ms, Time: FormatMs(ms)}
}

// BackfillPlan 补全时间计划
type BackfillPlan struct {
	HasEnd       bool          `json:"hasEnd"`
	IntervalMs   int64         `json:"intervalMs"`
	LagMs        int64         `json:"lagMs"`
	RangeStart   TimePointMs   `json:"rangeStart"`  // 整体起始边界（对齐后的毫秒点）
	RangeEnd     TimePointMs   `json:"rangeEnd"`    // 整体结束边界（对齐后的毫秒点）
	FirstWindow  TimeRangeMs   `json:"firstWindow"` // 第一个窗口 [start, end)
	LastWindow   TimeRangeMs   `json:"lastWindow"`  // 最后一个窗口 [start, end)
	Windows      []TimeRangeMs `json:"windows"`     // 全部窗口
	TotalWindows int           `json:"totalWindows"`
}

// CalcBackfillPlan 补全接口时间计算
// startMs: 必填；endMs: 0 表示不带结束时间，自动取当前 lag 边界
func CalcBackfillPlan(startMs, endMs int64, intervalSec, lagSec int, now time.Time) (*BackfillPlan, error) {
	intervalMs := IntervalMs(intervalSec)
	lagMsVal := LagMs(lagSec)
	if startMs <= 0 {
		return nil, fmt.Errorf("start 无效")
	}

	hasEnd := endMs > 0
	rangeStartMs := AlignFloorMs(startMs, intervalMs)

	var rangeEndMs int64
	if hasEnd {
		rangeEndMs = AlignCeilMs(endMs, intervalMs)
	} else {
		// 不带结束时间：截止到当前 (now-lag) 的上一窗口结束时刻
		_, rangeEndMs = PrevWindowMs(now.UnixMilli()-lagMsVal, intervalMs)
	}

	if rangeEndMs <= rangeStartMs {
		return nil, fmt.Errorf("时间范围无效 startMs=%d endMs=%d", startMs, rangeEndMs)
	}

	windows := SplitWindowsMs(rangeStartMs, rangeEndMs, intervalMs)
	if len(windows) == 0 {
		return nil, fmt.Errorf("未生成任何窗口")
	}

	return &BackfillPlan{
		HasEnd:       hasEnd,
		IntervalMs:   intervalMs,
		LagMs:        lagMsVal,
		RangeStart:   NewTimePointMs(rangeStartMs),
		RangeEnd:     NewTimePointMs(rangeEndMs),
		FirstWindow:  windows[0],
		LastWindow:   windows[len(windows)-1],
		Windows:      windows,
		TotalWindows: len(windows),
	}, nil
}

// CompareSide 单侧查询范围
type CompareSide struct {
	StartMs int64  `json:"startMs"`
	EndMs   int64  `json:"endMs"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Field   string `json:"field"`
	Count   int    `json:"count"`
}

// CompareResult 对比结果
type CompareResult struct {
	Range  TimeRangeMs `json:"range"`
	ES     CompareSide `json:"es"`
	MySQL  CompareSide `json:"mysql"`
	Diff   int         `json:"diff"` // es - mysql
	Match  bool        `json:"match"`
}

// LastHourCompareWindow 最近 1 小时对比范围（对齐 interval，结束于 lag 边界）
func LastHourCompareWindow(now time.Time, intervalSec, lagSec int) TimeRangeMs {
	intervalMs := IntervalMs(intervalSec)
	lagMs := LagMs(lagSec)
	_, endMs := PrevWindowMs(now.UnixMilli()-lagMs, intervalMs)
	startMs := AlignFloorMs(endMs-3600000, intervalMs)
	return NewTimeRangeMs(startMs, endMs)
}

// CalcCompareRange 计算对比时间范围
// - 无参：最近 1 小时（对齐 interval，结束于 lag 边界）
// - 仅 start：包含该时刻的对齐窗口 [floor, floor+interval)
// - start+end：对齐后的完整区间
func CalcCompareRange(startMs, endMs int64, intervalSec, lagSec int, now time.Time) (TimeRangeMs, error) {
	intervalMs := IntervalMs(intervalSec)
	if startMs <= 0 && endMs <= 0 {
		return LastHourCompareWindow(now, intervalSec, lagSec), nil
	}
	if startMs > 0 && endMs <= 0 {
		s := AlignFloorMs(startMs, intervalMs)
		return NewTimeRangeMs(s, s+intervalMs), nil
	}
	if startMs > 0 && endMs > 0 {
		s := AlignFloorMs(startMs, intervalMs)
		e := AlignCeilMs(endMs, intervalMs)
		if e <= s {
			return TimeRangeMs{}, fmt.Errorf("时间范围无效")
		}
		return NewTimeRangeMs(s, e), nil
	}
	return TimeRangeMs{}, fmt.Errorf("请提供 start，或留空使用增量默认窗口")
}
