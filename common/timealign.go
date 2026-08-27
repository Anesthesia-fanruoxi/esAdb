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

// PrevWindow 上一已结束时间窗 [floor-interval, floor)
// 例：interval=10，当前 17:28:01 → [17:27:50, 17:28:00)
func PrevWindow(t time.Time, intervalSec int) (start, end time.Time) {
	end = AlignFloor(t, intervalSec)
	start = end.Add(-time.Duration(intervalSec) * time.Second)
	return start, end
}

// TickSeconds 增量巡检周期 = interval/3，至少 1 秒
func TickSeconds(intervalSec int) int {
	if intervalSec <= 0 {
		intervalSec = 10
	}
	t := intervalSec / 3
	if t < 1 {
		t = 1
	}
	return t
}

// WindowKey 窗口去重键
func WindowKey(start, end time.Time) string {
	return fmt.Sprintf("%d_%d", start.Unix(), end.Unix())
}
