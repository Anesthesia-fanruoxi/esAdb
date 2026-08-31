package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config 全局配置
type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Log    LogConfig    `mapstructure:"log"`
	ES     ESConfig     `mapstructure:"es"`
	MySQL  MySQLConfig  `mapstructure:"mysql"`
	Sync   SyncConfig   `mapstructure:"sync"`

	Ready bool   `mapstructure:"-"`
	Tip   string `mapstructure:"-"`
}

type ServerConfig struct {
	Addr string `mapstructure:"addr"`
}

// LogConfig 日志
type LogConfig struct {
	Level string `mapstructure:"level"` // debug|info|warn|error|off，默认 off
}

type ESConfig struct {
	URL         string `mapstructure:"url"`
	Index       string `mapstructure:"index"`
	Username    string `mapstructure:"username"`
	Password    string `mapstructure:"password"`
	QueryString string `mapstructure:"query_string"` // ES query_string，如 method:addEventLog
	Fields      string `mapstructure:"fields"`       // 解析字段名，如 content
	DateField   string `mapstructure:"dateField"`    // 日期字段，默认 @timestamp
	Strip       string `mapstructure:"strip"`        // 正文事件前缀标识，识别并去掉头部，默认 用户事件记录===
}

// ParseField 返回实际使用的解析字段名
func (e ESConfig) ParseField() string {
	if s := strings.TrimSpace(e.Fields); s != "" {
		return s
	}
	return "content"
}

// StripPrefix 返回正文事件前缀标识（用于识别并去掉内容头部）
func (e ESConfig) StripPrefix() string {
	if s := strings.TrimSpace(e.Strip); s != "" {
		return s
	}
	return "用户事件记录==="
}

type MySQLConfig struct {
	DSN      string `mapstructure:"dsn"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	Table    string `mapstructure:"table"`
}

type SyncConfig struct {
	Interval         int `mapstructure:"interval"`           // 同步窗口（秒）
	LagSeconds       int `mapstructure:"lag_seconds"`        // ES 写入延迟补偿（秒），默认 60
	MaxSize          int `mapstructure:"max_size"`           // ES 单窗口最大拉取条数
	BatchSize        int `mapstructure:"batch_size"`         // ADB 单条 REPLACE 最大行数，默认与 max_size 一致
	MaxTime          int `mapstructure:"max_time"`           // 同步最大时间（秒）
	MaxRetry         int `mapstructure:"max_retry"`          // 最大重试次数
	RetryDelay       int `mapstructure:"retry_delay"`        // 重试延迟（秒）
	RetryDelayMax    int `mapstructure:"retry_delay_max"`    // 重试最大延迟（秒）
	BackfillWorkers  int `mapstructure:"backfill_workers"`   // 补全并行 worker 数，默认 2（限流，避免打满）
	BackfillPauseMs  int `mapstructure:"backfill_pause_ms"`  // 每个窗口完成后暂停毫秒，默认 50；0=不停
	BackfillBatch    int `mapstructure:"backfill_batch"`     // 范围补全单批拉取条数（search_after 分页），默认 10000
}

var global *Config

func Get() *Config {
	if global == nil {
		return &Config{
			Ready: false,
			Tip:   "无配置运行中，不会做任何操作",
		}
	}
	return global
}

// Load 环境变量 > yaml；文件缺失或为空不报错
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	v.SetEnvPrefix("ESADB")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("server.addr", ":8080")
	v.SetDefault("log.level", "off")
	v.SetDefault("es.index", "ysh-ysh-app-info*")
	v.SetDefault("es.query_string", "method:addEventLog")
	v.SetDefault("es.fields", "content")
	v.SetDefault("es.dateField", "@timestamp")
	v.SetDefault("es.strip", "用户事件记录===")
	v.SetDefault("mysql.port", 3306)
	v.SetDefault("mysql.table", "app_event_log")
	v.SetDefault("sync.interval", 10)
	v.SetDefault("sync.lag_seconds", 60)
	v.SetDefault("sync.max_size", 1000)
	v.SetDefault("sync.batch_size", 1000)
	v.SetDefault("sync.max_time", 10)
	v.SetDefault("sync.max_retry", 3)
	v.SetDefault("sync.retry_delay", 1)
	v.SetDefault("sync.retry_delay_max", 10)
	v.SetDefault("sync.backfill_workers", 2)
	v.SetDefault("sync.backfill_pause_ms", 50)
	v.SetDefault("sync.backfill_batch", 10000)

	cfg := &Config{}
	if path == "" {
		path = "config/config.yaml"
	}

	if _, err := os.Stat(path); err == nil {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			cfg.Tip = fmt.Sprintf("配置文件读取失败(%v)，无配置运行中，不会做任何操作", err)
			cfg.Ready = false
			global = cfg
			return cfg, nil
		}
	} else {
		cfg.Tip = "未找到配置文件，无配置运行中，不会做任何操作"
	}

	keys := []string{
		"server.addr",
		"log.level",
		"es.url", "es.index", "es.username", "es.password", "es.query_string",
		"es.fields", "es.dateField", "es.strip",
		"mysql.dsn", "mysql.host", "mysql.port", "mysql.user", "mysql.password", "mysql.database", "mysql.table",
		"sync.interval", "sync.lag_seconds", "sync.max_size", "sync.batch_size", "sync.max_time", "sync.max_retry",
		"sync.retry_delay", "sync.retry_delay_max", "sync.backfill_workers", "sync.backfill_pause_ms", "sync.backfill_batch",
	}
	for _, k := range keys {
		_ = v.BindEnv(k)
	}

	if err := v.Unmarshal(cfg); err != nil {
		cfg.Tip = fmt.Sprintf("配置解析失败(%v)，无配置运行中，不会做任何操作", err)
		cfg.Ready = false
		global = cfg
		return cfg, nil
	}

	cfg.normalize()
	cfg.evaluateReady()
	global = cfg
	return cfg, nil
}

func (c *Config) normalize() {
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.ES.Index == "" {
		c.ES.Index = "ysh-ysh-app-info*"
	}
	if c.ES.QueryString == "" {
		c.ES.QueryString = "method:addEventLog"
	}
	if c.ES.DateField == "" {
		c.ES.DateField = "@timestamp"
	}
	if c.ES.Strip == "" {
		c.ES.Strip = "用户事件记录==="
	}
	if c.MySQL.Port == 0 {
		c.MySQL.Port = 3306
	}
	if c.MySQL.Table == "" {
		c.MySQL.Table = "app_event_log"
	}
	if c.Sync.Interval <= 0 {
		c.Sync.Interval = 10
	}
	if c.Sync.LagSeconds <= 0 {
		c.Sync.LagSeconds = 60
	}
	if c.Sync.MaxSize <= 0 {
		c.Sync.MaxSize = 1000
	}
	if c.Sync.BatchSize <= 0 {
		c.Sync.BatchSize = c.Sync.MaxSize
	}
	if c.Sync.MaxRetry <= 0 {
		c.Sync.MaxRetry = 3
	}
	if c.Sync.RetryDelay <= 0 {
		c.Sync.RetryDelay = 1
	}
	if c.Sync.RetryDelayMax <= 0 {
		c.Sync.RetryDelayMax = 10
	}
	if c.Sync.BackfillWorkers <= 0 {
		c.Sync.BackfillWorkers = 2
	}
	if c.Sync.BackfillPauseMs < 0 {
		c.Sync.BackfillPauseMs = 50
	}
	if c.Sync.BackfillBatch <= 0 {
		c.Sync.BackfillBatch = 10000
	}
	if c.MySQL.DSN == "" && c.MySQL.Host != "" && c.MySQL.User != "" && c.MySQL.Database != "" {
		c.MySQL.DSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			c.MySQL.User, c.MySQL.Password, c.MySQL.Host, c.MySQL.Port, c.MySQL.Database)
	}
}

func (c *Config) evaluateReady() {
	if !c.HasES() && !c.HasMySQL() {
		c.Ready = false
		if c.Tip == "" {
			c.Tip = "无配置运行中，不会做任何操作"
		}
		return
	}
	c.Ready = true
	c.Tip = ""
}

func (c *Config) HasES() bool {
	return strings.TrimSpace(c.ES.URL) != ""
}

func (c *Config) HasMySQL() bool {
	if strings.TrimSpace(c.MySQL.DSN) != "" {
		return true
	}
	return strings.TrimSpace(c.MySQL.Host) != "" && strings.TrimSpace(c.MySQL.Database) != ""
}
