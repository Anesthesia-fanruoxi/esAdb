package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"strings"
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
	URL       string `mapstructure:"url"`
	Index     string `mapstructure:"index"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
	Method    string `mapstructure:"method"`
	Fields    string `mapstructure:"fields"`    // 解析字段名，如 content
	DateField string `mapstructure:"dateField"` // 日期字段，默认 @timestamp
}

// ParseField 返回实际使用的解析字段名
func (e ESConfig) ParseField() string {
	if s := strings.TrimSpace(e.Fields); s != "" {
		return s
	}
	return "content"
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
	Interval      int `mapstructure:"interval"`        // 同步间隔（秒）
	MaxSize       int `mapstructure:"max_size"`        // 单次最大条数
	MaxTime       int `mapstructure:"max_time"`        // 同步最大时间（秒）
	MaxRetry      int `mapstructure:"max_retry"`       // 最大重试次数
	RetryDelay    int `mapstructure:"retry_delay"`     // 重试延迟（秒）
	RetryDelayMax int `mapstructure:"retry_delay_max"` // 重试最大延迟（秒）
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
	v.SetDefault("es.method", "addEventLog")
	v.SetDefault("es.fields", "content")
	v.SetDefault("es.dateField", "@timestamp")
	v.SetDefault("mysql.port", 3306)
	v.SetDefault("mysql.table", "app_event_log")
	v.SetDefault("sync.interval", 10)
	v.SetDefault("sync.max_size", 10000)
	v.SetDefault("sync.max_time", 10)
	v.SetDefault("sync.max_retry", 3)
	v.SetDefault("sync.retry_delay", 1)
	v.SetDefault("sync.retry_delay_max", 10)

	cfg := &Config{}
	if path == "" {
		path = "config.yaml"
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
		"es.url", "es.index", "es.username", "es.password", "es.method",
		"es.fields", "es.dateField",
		"mysql.dsn", "mysql.host", "mysql.port", "mysql.user", "mysql.password", "mysql.database", "mysql.table",
		"sync.interval", "sync.max_size", "sync.max_time", "sync.max_retry",
		"sync.retry_delay", "sync.retry_delay_max",
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
	if c.ES.Method == "" {
		c.ES.Method = "addEventLog"
	}
	if c.ES.DateField == "" {
		c.ES.DateField = "@timestamp"
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
	if c.Sync.MaxSize <= 0 {
		c.Sync.MaxSize = 10000
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
