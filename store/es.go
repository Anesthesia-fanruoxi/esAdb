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
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// SearchByRange 查询 [start, end) 内 method 匹配的文档；logQuery 控制是否打印查询日志
func (s *ESStore) SearchByRange(start, end time.Time, size int, logQuery bool) ([]ESRecord, error) {
	if strings.TrimSpace(s.cfg.URL) == "" {
		return nil, fmt.Errorf("ES 未配置")
	}
	if size <= 0 {
		size = 10000
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
	for _, hit := range esResp.Hits.Hits {
		content, ok := hit.Source[parseField].(string)
		if !ok || !strings.Contains(content, "用户事件记录===") {
			continue
		}
		idx := strings.Index(content, "用户事件记录===")
		ts := parseESTimestamp(hit.Source[dateField])
		out = append(out, ESRecord{
			Content:     content[idx:],
			EsTimestamp: ts,
		})
	}
	if logQuery {
		common.Info("ES 区间查询 [%s, %s) hits=%d",
			start.Format("15:04:05"), end.Format("15:04:05"), len(out))
	}
	return out, nil
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
