package store

import (
	"runtime"
	"time"

	"esAdb/common"
)

var startedAt = time.Now()

// RuntimeStats 进程运行时统计（SSE runtime 事件）
type RuntimeStats struct {
	At            int64   `json:"at"`
	AtStr         string  `json:"atStr"`
	UptimeSec     int64   `json:"uptimeSec"`
	Goroutines    int     `json:"goroutines"`
	NumCPU        int     `json:"numCPU"`
	GoMaxProcs    int     `json:"goMaxProcs"`
	GoVersion     string  `json:"goVersion"`
	AllocMB       float64 `json:"allocMB"`
	TotalAllocMB  float64 `json:"totalAllocMB"`
	SysMB         float64 `json:"sysMB"`
	HeapSysMB     float64 `json:"heapSysMB"`
	HeapObjects   uint64  `json:"heapObjects"`
	NumGC         uint32  `json:"numGC"`
	PauseTotalSec float64 `json:"pauseTotalSec"`
}

// RuntimeStatsNow 采集当前进程运行时指标
func RuntimeStatsNow() RuntimeStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	now := time.Now()
	return RuntimeStats{
		At:            now.UnixMilli(),
		AtStr:         common.FormatMs(now.UnixMilli()),
		UptimeSec:     int64(now.Sub(startedAt).Seconds()),
		Goroutines:    runtime.NumGoroutine(),
		NumCPU:        runtime.NumCPU(),
		GoMaxProcs:    runtime.GOMAXPROCS(0),
		GoVersion:     runtime.Version(),
		AllocMB:       float64(ms.Alloc) / (1 << 20),
		TotalAllocMB:  float64(ms.TotalAlloc) / (1 << 20),
		SysMB:         float64(ms.Sys) / (1 << 20),
		HeapSysMB:     float64(ms.HeapSys) / (1 << 20),
		HeapObjects:   ms.HeapObjects,
		NumGC:         ms.NumGC,
		PauseTotalSec: float64(ms.PauseTotalNs) / 1e9,
	}
}