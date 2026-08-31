package store

import (
	"fmt"
	"reflect"
	"time"

	"esAdb/common"
)

// BackfillRangePaged 范围补全（全量分页模式）：
// 不切窗口，整段范围 search_after 复合排序翻页，每批 batch 条：查 ES → 解析去重 → REPLACE INTO ADB。
// 失败批次记录时间区间（事后可用窗口补全修复）；estBatches 为外部预估批次数（进度分母），<=0 时内部自行估算。
func (m *Manager) BackfillRangePaged(startMs, endMs int64, batch, estBatches int) *BackfillSummary {
	summary := &BackfillSummary{}
	if m == nil || m.ES == nil || m.MySQL == nil {
		return summary
	}
	if batch <= 0 {
		batch = 10000
	}

	totalBatches := estBatches
	if totalBatches <= 0 {
		totalDocs, err := m.ES.CountByRangeMs(startMs, endMs)
		if err != nil {
			common.Warn("[backfill-paged] 总数预估失败（不影响补全）: %v", err)
		}
		if totalDocs > 0 {
			totalBatches = int((int64(totalDocs) + int64(batch) - 1) / int64(batch))
		}
	}
	if totalBatches <= 0 {
		totalBatches = 1
	}

	if m.Monitor != nil {
		m.Monitor.BeginBackfill(common.FormatMs(startMs), common.FormatMs(endMs), totalBatches,
			common.NewTimeRangeMs(startMs, endMs), common.NewTimeRangeMs(startMs, endMs))
		defer m.Monitor.EndBackfill()
	}

	pause := time.Duration(m.cfg.Sync.BackfillPauseMs) * time.Millisecond
	retryDelay := time.Duration(m.cfg.Sync.RetryDelay) * time.Second
	retry := m.cfg.Sync.MaxRetry
	if retry < 0 {
		retry = 0
	}
	common.Info("[backfill-paged] 开始 range=[%s, %s) batch=%d estBatches=%d",
		common.FormatMs(startMs), common.FormatMs(endMs), batch, totalBatches)

	var (
		cursor  []interface{}
		batchNo int
	)
	begin := time.Now()
	for {
		batchNo++
		res := WindowSyncResult{Window: common.NewTimeRangeMs(startMs, endMs)}
		batchStart := time.Now()

		// ES 分页查询（带重试）
		var records []ESRecord
		var next []interface{}
		var lastErr error
		for attempt := 0; ; attempt++ {
			records, next, lastErr = m.ES.SearchPaged(startMs, endMs, int64(batch), cursor)
			if lastErr == nil || attempt >= retry {
				break
			}
			common.Warn("[backfill-paged] 批次 %d ES 查询失败(%d/%d): %v",
				batchNo, attempt+1, retry, lastErr)
			time.Sleep(retryDelay)
		}
		if lastErr != nil {
			res.Error = fmt.Sprintf("ES 查询失败: %v", lastErr)
			// 记录真实断点：从当前游标到范围末尾（供窗口补全修复）
			if len(cursor) > 0 {
				var curTs int64
				switch v := cursor[0].(type) {
				case float64:
					curTs = int64(v)
				case int64:
					curTs = v
				}
				if curTs > 0 {
					res.Window = common.NewTimeRangeMs(curTs, endMs)
				}
			}
		}

		// 解析 + 写 ADB（带重试）
		if lastErr == nil && len(records) > 0 {
			logs := BuildFromESRecords(records)
			written, _, werr := m.MySQL.BatchInsertIgnore(logs)
			for attempt := 0; werr != nil && attempt < retry; attempt++ {
				common.Warn("[backfill-paged] 批次 %d ADB 写入失败(%d/%d): %v",
					batchNo, attempt+1, retry, werr)
				time.Sleep(retryDelay)
				written, _, werr = m.MySQL.BatchInsertIgnore(logs)
			}
			if werr != nil {
				res.Error = fmt.Sprintf("ADB 写入失败: %v", werr)
			}
			res.Hits = len(records)
			res.Written = written
			// 批次时间区间 = 本批首尾 es_timestamp（失败批次可据此用窗口补全修复）
			res.Window = common.NewTimeRangeMs(records[0].EsTimestamp, records[len(records)-1].EsTimestamp+1)
		}
		res.DurationMs = time.Since(batchStart).Milliseconds()

		if res.Error != "" {
			summary.Failed++
			summary.Windows = append(summary.Windows, res)
		} else {
			summary.TotalWindows++
		}
		summary.TotalHits += res.Hits
		summary.TotalWritten += res.Written

		if m.Monitor != nil {
			m.Monitor.RecordBackfillWindow(res)
			m.Monitor.UpdateBackfillProgress(int(summary.TotalWindows), summary.Failed,
				summary.TotalHits, summary.TotalWritten, totalBatches,
				common.FormatMs(startMs), common.FormatMs(endMs))
		}
		common.Debug("[backfill-paged] 批次 %d hits=%d written=%d 耗时=%dms",
			batchNo, res.Hits, res.Written, res.DurationMs)

		// 终止：本页游标为空（已到范围末尾）；ES 查询失败时游标为空，
		// 剩余区间已记入失败批次（res.Window），可用窗口补全修复
		if len(next) == 0 {
			if lastErr != nil {
				common.Error("[backfill-paged] ES 查询失败终止，剩余区间 [%s, %s) 未补全",
					common.FormatMs(res.Window.StartMs), common.FormatMs(endMs))
			}
			break
		}
		// 异常保护：游标未推进则终止，避免死循环
		if reflect.DeepEqual(cursor, next) {
			common.Warn("[backfill-paged] 批次 %d 游标未推进，终止", batchNo)
			break
		}
		cursor = next
		if pause > 0 {
			time.Sleep(pause)
		}
	}
	summary.TotalWindows = batchNo
	common.Info("[backfill-paged] 完成 批次=%d hits=%d written=%d failed=%d 耗时=%s",
		batchNo, summary.TotalHits, summary.TotalWritten, summary.Failed,
		time.Since(begin).Round(time.Second))
	return summary
}
