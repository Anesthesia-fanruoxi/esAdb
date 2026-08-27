package model

import "time"

// tableName 运行时表名，由配置 mysql.table 注入
var tableName = "app_event_log"

// SetTableName 设置表名
func SetTableName(name string) {
	if name != "" {
		tableName = name
	}
}

// GetTableName 获取当前表名
func GetTableName() string { return tableName }

// EventLog 用户事件记录（对应 ADB 表 app_event_log）
type EventLog struct {
	ID             string    `db:"id" json:"id"`
	Type           string    `db:"type" json:"type"`
	PhoneMd5       string    `db:"phone_md5" json:"phoneMd5"`
	CustomerID     string    `db:"customer_id" json:"customerId"`
	EventID        string    `db:"event_id" json:"eventId"`
	EventName      string    `db:"event_name" json:"eventName"`
	ChannelID      string    `db:"channel_id" json:"channelId"`
	ChannelName    string    `db:"channel_name" json:"channelName"`
	Client         string    `db:"client" json:"client"`
	Path           string    `db:"path" json:"path"`
	IP             string    `db:"ip" json:"ip"`
	CreateTime     time.Time `db:"create_time" json:"createTime"`
	Source         string    `db:"source" json:"source"`
	AppVersionType string    `db:"app_version_type" json:"appVersionType"`
	Extend         string    `db:"extend" json:"extend"`
	EsTimestamp    int64     `db:"es_timestamp" json:"esTimestamp"` // ES @timestamp，Unix 毫秒
}

// Columns INSERT 字段顺序
var Columns = []string{
	"id", "type", "phone_md5", "customer_id", "event_id", "event_name",
	"channel_id", "channel_name", "client", "path", "ip", "create_time",
	"source", "app_version_type", "extend", "es_timestamp",
}

// KeyToCol 解析字段名 -> 表列名
var KeyToCol = map[string]string{
	"id":             "id",
	"type":           "type",
	"phoneMd5":       "phone_md5",
	"customerId":     "customer_id",
	"eventId":        "event_id",
	"eventName":      "event_name",
	"channelId":      "channel_id",
	"channelName":    "channel_name",
	"client":         "client",
	"path":           "path",
	"ip":             "ip",
	"createTime":     "create_time",
	"source":         "source",
	"appVersionType": "app_version_type",
	"extend":         "extend",
	"esTimestamp":    "es_timestamp",
}
