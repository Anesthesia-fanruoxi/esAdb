package common

import (
	"fmt"
	"strings"
	"time"

	"esAdb/model"
)

// ConvertMap 解析 "用户事件记录===id:xxx,...,extend:..."（与 Java convertMap 一致）
func ConvertMap(content string) map[string]string {
	result := make(map[string]string)
	if content == "" {
		return result
	}

	parts := strings.SplitN(content, "===", 2)
	if len(parts) <= 1 {
		return result
	}
	body := strings.TrimSpace(parts[1])
	if body == "" {
		return result
	}

	var extendValue *string
	extendIdx := strings.Index(body, "extend:")
	var kvPart string
	if extendIdx != -1 {
		kvPart = body[:extendIdx]
		v := strings.TrimSpace(body[extendIdx+len("extend:"):])
		extendValue = &v
	} else {
		kvPart = body
	}

	for _, field := range strings.Split(kvPart, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		colonIndex := strings.Index(field, ":")
		if colonIndex == -1 {
			continue
		}
		key := strings.TrimSpace(field[:colonIndex])
		value := strings.TrimSpace(field[colonIndex+1:])
		if key == "" {
			continue
		}
		result[key] = value
	}

	if extendValue != nil {
		result["extend"] = *extendValue
	}
	return result
}

func nullToEmpty(s string) string {
	if s == "null" || s == "" {
		return ""
	}
	return s
}

// MapToEventLog map -> 模型
func MapToEventLog(m map[string]string) (*model.EventLog, error) {
	if m["id"] == "" {
		return nil, fmt.Errorf("缺少 id")
	}
	el := &model.EventLog{
		ID:             m["id"],
		Type:           m["type"],
		PhoneMd5:       m["phoneMd5"],
		CustomerID:     m["customerId"],
		EventID:        m["eventId"],
		EventName:      m["eventName"],
		ChannelID:      m["channelId"],
		ChannelName:    m["channelName"],
		Client:         m["client"],
		Path:           m["path"],
		IP:             m["ip"],
		Source:         m["source"],
		AppVersionType: m["appVersionType"],
		Extend:         nullToEmpty(m["extend"]),
	}
	if ct := m["createTime"]; ct != "" {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", ct, time.Local)
		if err != nil {
			return nil, fmt.Errorf("createTime 解析失败: %w", err)
		}
		el.CreateTime = t
	}
	return el, nil
}
