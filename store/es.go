package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"esAdb/common"
	"esAdb/config"
)

// ESStore ES 操作
type ESStore struct {
	cfg config.ESConfig
}

func NewESStore(cfg config.ESConfig) *ESStore {
	return &ESStore{cfg: cfg}
}

// ESRecord 一条 ES 命中：解析正文 + ES 毫秒时间戳
type ESRecord struct {
	Content     string
	EsTimestamp int64 // Unix 毫秒
}

type esSearchResp struct {
	Hits struct {
		Total json.RawMessage `json:"total"` // ES7+: {"value":N}；旧版/兼容: 数字
		Hits  []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// parseHitsTotal 兼容 ES total 两种格式
func parseHitsTotal(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	var obj struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		return obj.Value
	}
	return 0
}

// SearchByRangeMs 按毫秒时间戳查询 [startMs, endMs)
func (s *ESStore) SearchByRangeMs(startMs, endMs int64, size int, logQuery bool) ([]ESRecord, error) {
	return s.SearchByRange(time.UnixMilli(startMs), time.UnixMilli(endMs), size, logQuery)
}

// SearchByRange 查询 [start, end) 内 method 匹配的文档；logQuery 控制是否打印查询日志
func (s *ESStore) SearchByRange(start, end time.Time, size int, logQuery bool) ([]ESRecord, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return nil, fmt.Errorf("ES 未配置")
	}
	if size <= 0 {
		size = 1000
	}
	method := s.cfg.Method
	if method == "" {
		method = "addEventLog"
	}
	dateField := s.cfg.DateField
	if dateField == "" {
		dateField = "@timestamp"
	}
	parseField := s.cfg.ParseField()

	query := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"method.keyword": method,
						},
					},
					map[string]interface{}{
						"range": map[string]interface{}{
							dateField: map[string]interface{}{
								"gte": start.UTC().Format(time.RFC3339Nano),
								"lt":  end.UTC().Format(time.RFC3339Nano),
							},
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{dateField: map[string]string{"order": "asc"}},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	url := strings.TrimRight(s.cfg.URL, "/") + "/" + s.cfg.Index + "/_search"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Username != "" {
		req.SetBasicAuth(s.cfg.Username, s.cfg.Password)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ES 查询失败 status=%d body=%s", resp.StatusCode, string(raw))
	}

	var esResp esSearchResp
	if err := json.Unmarshal(raw, &esResp); err != nil {
		return nil, err
	}

	var out []ESRecord
	strip := s.cfg.StripPrefix()
	for _, hit := range esResp.Hits.Hits {
		content, ok := hit.Source[parseField].(string)
		if !ok || !strings.Contains(content, strip) {
			continue
		}
		idx := strings.Index(content, strip)
		ts := parseESTimestamp(hit.Source[dateField])
		out = append(out, ESRecord{
			Content:     content[idx:],
			EsTimestamp: ts,
		})
	}
	if logQuery {
		common.Debug("ES 区间查询 [%s, %s) hits=%d",
			start.Format("15:04:05"), end.Format("15:04:05"), len(out))
	}
	return out, nil
}

// CountByRangeMs 统计 [startMs, endMs) 文档数（与 SearchByRange 同一 ES 条件，不含客户端 content 过滤）
func (s *ESStore) CountByRangeMs(startMs, endMs int64) (int, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return 0, fmt.Errorf("ES 未配置")
	}
	method := s.cfg.Method
	if method == "" {
		method = "addEventLog"
	}
	dateField := s.cfg.DateField
	if dateField == "" {
		dateField = "@timestamp"
	}

	start := time.UnixMilli(startMs)
	end := time.UnixMilli(endMs)

	// 与 SearchByRange 保持一致：仅 method + 时间范围（勿对 text 字段做 wildcard）
	query := map[string]interface{}{
		"size":             0,
		"track_total_hits": true,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"method.keyword": method,
						},
					},
					map[string]interface{}{
						"range": map[string]interface{}{
							dateField: map[string]interface{}{
								"gte": start.UTC().Format(time.RFC3339Nano),
								"lt":  end.UTC().Format(time.RFC3339Nano),
							},
						},
					},
				},
			},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return 0, err
	}
	url := strings.TrimRight(s.cfg.URL, "/") + "/" + s.cfg.Index + "/_search"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Username != "" {
		req.SetBasicAuth(s.cfg.Username, s.cfg.Password)
	}
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ES count 失败 status=%d body=%s", resp.StatusCode, string(raw))
	}
	var esResp esSearchResp
	if err := json.Unmarshal(raw, &esResp); err != nil {
		return 0, err
	}
	n := parseHitsTotal(esResp.Hits.Total)
	common.Debug("ES 对比统计 [%s, %s) count=%d",
		start.Format("15:04:05"), end.Format("15:04:05"), n)
	return n, nil
}

func parseESTimestamp(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case string:
		formats := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02 15:04:05",
		}
		for _, f := range formats {
			if tm, err := time.Parse(f, t); err == nil {
				return tm.UnixMilli()
			}
		}
		if tm, err := time.ParseInLocation("2006-01-02 15:04:05", t, time.Local); err == nil {
			return tm.UnixMilli()
		}
	case float64:
		// 已是毫秒（约 13 位）则直接用；秒则 *1000
		if t > 1e12 {
			return int64(t)
		}
		return int64(t * 1000)
	case json.Number:
		f, _ := t.Float64()
		if f > 1e12 {
			return int64(f)
		}
		return int64(f * 1000)
	}
	return 0
}
