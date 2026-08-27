package common

import (
	"fmt"
	"time"
)

// AlignFloor 从 Unix 0 点起向下对齐：aligned = unix - unix%interval
func AlignFloor(t time.Time, intervalSec int) time.Time {
	if intervalSec <= 0 {
		intervalSec = 10
	}
	unix := t.Unix()
	aligned := unix - (unix % int64(intervalSec))
	return time.Unix(aligned, 0).In(t.Location())
}

// NextAlign 下一刻准点（已在准点则再推一个 interval）
func NextAlign(t time.Time, intervalSec int) time.Time {
	if intervalSec <= 0 {
		intervalSec = 10
	}
	unix := t.Unix()
	mod := unix % int64(intervalSec)
	if mod == 0 {
		return time.Unix(unix+int64(intervalSec), 0).In(t.Location())
	}
	return time.Unix(unix-mod+int64(intervalSec), 0).In(t.Location())
}

// AlignCeil 向上对齐；已在准点则返回自身
func AlignCeil(t time.Time, intervalSec int) time.Time {
	if intervalSec <= 0 {
		intervalSec = 10
	}
	unix := t.Unix()
	mod := unix % int64(intervalSec)
	if mod == 0 {
		return time.Unix(unix, 0).In(t.Location())
	}
	return time.Unix(unix-mod+int64(intervalSec), 0).In(t.Location())
}

// PrevWindow 相对当前时刻的上一已结束窗 [floor-interval, floor)
// floor 为当前所在周期的起点（上一周期终点）；例 interval=10，18:50:15 → [18:50:00,18:50:10)
func PrevWindow(t time.Time, intervalSec int) (start, end time.Time) {
	end = AlignFloor(t, intervalSec)
	start = end.Add(-time.Duration(intervalSec) * time.Second)
	return start, end
}

// CountWindows [start,end) 内按 interval 切分的窗口总数
func CountWindows(start, end time.Time, intervalSec int) int64 {
	if intervalSec <= 0 {
		intervalSec = 10
	}
	var n int64
	for cur := start; cur.Before(end); cur = cur.Add(time.Duration(intervalSec) * time.Second) {
		n++
	}
	return n
}

// WindowKey 窗口去重键
func WindowKey(start, end time.Time) string {
	return fmt.Sprintf("%d_%d", start.Unix(), end.Unix())
}
