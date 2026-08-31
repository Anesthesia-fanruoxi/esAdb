package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// SearchPaged search_after 分页查询（范围补全专用）。
// 复合排序 [dateField, _id]：同时间戳（如整点堆积批次）按 _id 续翻，不丢不重；
// after 为上一页最后一条的 sort 值，nil 表示首页。
// 返回：过滤后的记录 + 下一页游标（取自本页最后一条的 sort 值）。
func (s *ESStore) SearchPaged(startMs, endMs, batch int64, after []interface{}) ([]ESRecord, []interface{}, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return nil, nil, fmt.Errorf("ES 未配置")
	}
	if batch <= 0 {
		batch = 10000
	}
	df := s.dateField()

	bodyMap := map[string]interface{}{
		"size": batch,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					s.queryStringClause(),
					map[string]interface{}{
						"range": map[string]interface{}{
							df: map[string]interface{}{
								"gte": time.UnixMilli(startMs).UTC().Format(time.RFC3339Nano),
								"lt":  time.UnixMilli(endMs).UTC().Format(time.RFC3339Nano),
							},
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{df: map[string]interface{}{"order": "asc"}},
			{"_id": map[string]interface{}{"order": "asc", "unmapped_type": "keyword"}},
		},
	}
	if len(after) > 0 {
		bodyMap["search_after"] = after
	}

	body, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, nil, err
	}
	raw, err := s.doSearch(body, 120*time.Second)
	if err != nil {
		return nil, nil, err
	}

	var esResp esSearchResp
	if err := json.Unmarshal(raw, &esResp); err != nil {
		return nil, nil, err
	}

	hits := esResp.Hits.Hits
	out := s.recordsFromHits(hits)

	// 游标取本页最后一条的 sort 值；缺失时用 [ts, _id] 手动兜底
	var cursor []interface{}
	if n := len(hits); n > 0 {
		cursor = hits[n-1].Sort
		if len(cursor) == 0 {
			cursor = []interface{}{parseESTimestamp(hits[n-1].Source[df]), hits[n-1].ID}
		}
	}
	return out, cursor, nil
}