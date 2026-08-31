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

// esHit ES 单条命中
type esHit struct {
	ID     string                 `json:"_id"`
	Sort   []interface{}          `json:"sort"`
	Source map[string]interface{} `json:"_source"`
}

type esSearchResp struct {
	Hits struct {
		Total json.RawMessage `json:"total"` // ES7+: {"value":N}；旧版/兼容: 数字
		Hits  []esHit         `json:"hits"`
	} `json:"hits"`
}

// dateField 返回实际使用的日期字段
func (s *ESStore) dateField() string {
	if s.cfg.DateField == "" {
		return "@timestamp"
	}
	return s.cfg.DateField
}

// doSearch 统一发送 ES _search 请求，返回原始响应体（覆盖鉴权、超时与状态码检查）
func (s *ESStore) doSearch(body []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = 60 * time.Second
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
	client := &http.Client{Timeout: timeout}
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
	return raw, nil
}

// recordsFromHits 将 ES hits 解析为 ESRecord：按 strip 前缀过滤正文并去掉头部
func (s *ESStore) recordsFromHits(hits []esHit) []ESRecord {
	strip := s.cfg.StripPrefix()
	parseField := s.cfg.ParseField()
	df := s.dateField()
	out := make([]ESRecord, 0, len(hits))
	for _, hit := range hits {
		content, ok := hit.Source[parseField].(string)
		if !ok || !strings.Contains(content, strip) {
			continue
		}
		idx := strings.Index(content, strip)
		out = append(out, ESRecord{
			Content:     content[idx:],
			EsTimestamp: parseESTimestamp(hit.Source[df]),
		})
	}
	return out
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
	records, _, err := s.SearchByRangeMsRaw(startMs, endMs, size, logQuery)
	return records, err
}

// SearchByRangeMsRaw 按毫秒时间戳查询，返回过滤后记录与原始命中数。
// 原始命中数用于判断本窗口是否达到 size 上限（可能截断），自适应补全据此切小窗口。
func (s *ESStore) SearchByRangeMsRaw(startMs, endMs int64, size int, logQuery bool) ([]ESRecord, int, error) {
	if size <= 0 {
		size = 1000
	}
	return s.SearchByRange(time.UnixMilli(startMs), time.UnixMilli(endMs), size, logQuery)
}

// queryStringClause 使用配置 es.query_string（如 method:addEventLog）
func (s *ESStore) queryStringClause() map[string]interface{} {
	q := strings.TrimSpace(s.cfg.QueryString)
	if q == "" {
		q = "method:addEventLog"
	}
	return map[string]interface{}{
		"query_string": map[string]interface{}{
			"query": q,
		},
	}
}

// SearchByRange 查询 [start, end) 内 method 匹配的文档；logQuery 控制是否打印查询日志。
// 返回过滤后的记录与 ES 实际返回的命中条数（受 size 截断，用于判断窗口是否可能溢出）。
func (s *ESStore) SearchByRange(start, end time.Time, size int, logQuery bool) ([]ESRecord, int, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return nil, 0, fmt.Errorf("ES 未配置")
	}
	if size <= 0 {
		size = 1000
	}

	query := map[string]interface{}{
		"size": size,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					s.queryStringClause(),
					map[string]interface{}{
						"range": map[string]interface{}{
							s.dateField(): map[string]interface{}{
								"gte": start.UTC().Format(time.RFC3339Nano),
								"lt":  end.UTC().Format(time.RFC3339Nano),
							},
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{s.dateField(): map[string]string{"order": "asc"}},
		},
	}

	body, err := json.Marshal(query)
	if err != nil {
		return nil, 0, err
	}
	raw, err := s.doSearch(body, 60*time.Second)
	if err != nil {
		return nil, 0, err
	}

	var esResp esSearchResp
	if err := json.Unmarshal(raw, &esResp); err != nil {
		return nil, 0, err
	}

	hitCount := len(esResp.Hits.Hits)
	out := s.recordsFromHits(esResp.Hits.Hits)
	if logQuery {
		common.Debug("ES 区间查询 [%s, %s) hits=%d(截断阈值 %d)",
			start.Format("15:04:05"), end.Format("15:04:05"), hitCount, size)
	}
	return out, hitCount, nil
}

// CountByRangeMs 统计 [startMs, endMs) 文档数（与 SearchByRange 同一 ES 条件，不含客户端 content 过滤）
func (s *ESStore) CountByRangeMs(startMs, endMs int64) (int, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return 0, fmt.Errorf("ES 未配置")
	}

	start := time.UnixMilli(startMs)
	end := time.UnixMilli(endMs)

	query := map[string]interface{}{
		"size":             0,
		"track_total_hits": true,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					s.queryStringClause(),
					map[string]interface{}{
						"range": map[string]interface{}{
							s.dateField(): map[string]interface{}{
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
	raw, err := s.doSearch(body, 60*time.Second)
	if err != nil {
		return 0, err
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