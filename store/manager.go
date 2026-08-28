package store

import (
	"context"
	"sync"

	"esAdb/common"
	"esAdb/config"
)

// Manager 统一持有 ES / MySQL / Syncer / Monitor
type Manager struct {
	cfg     *config.Config
	ES      *ESStore
	MySQL   *MySQLStore
	Syncer  *Syncer
	Monitor *Monitor
}

var (
	mgrOnce sync.Once
	mgr     *Manager
)

// Init 按配置初始化；缺配置则跳过，不报错
func Init(cfg *config.Config) *Manager {
	mgrOnce.Do(func() {
		m := &Manager{cfg: cfg}
		if cfg.HasES() {
			m.ES = NewESStore(cfg.ES)
			common.Info("ES store 已就绪 url=%s index=%s field=%s dateField=%s",
				cfg.ES.URL, cfg.ES.Index, cfg.ES.ParseField(), cfg.ES.DateField)
		} else {
			common.Warn("ES 未配置，跳过初始化")
		}
		if cfg.HasMySQL() {
			ms, err := NewMySQLStore(cfg.MySQL, cfg.Sync.BatchSize)
			if err != nil {
				common.Error("MySQL 初始化失败: %v（将跳过 MySQL 操作）", err)
			} else {
				m.MySQL = ms
			}
		} else {
			common.Warn("MySQL 未配置，跳过初始化")
		}
		m.Syncer = NewSyncer(cfg, m)
		m.Monitor = NewMonitor(m)
		m.Monitor.Start()
		mgr = m
	})
	return mgr
}

func Get() *Manager { return mgr }

// StartIncremental 启动增量同步调度
func (m *Manager) StartIncremental(ctx context.Context) {
	if m == nil || m.Syncer == nil {
		return
	}
	if m.ES == nil || m.MySQL == nil {
		common.Warn("ES 或 MySQL 未就绪，跳过增量调度")
		return
	}
	m.Syncer.StartIncremental(ctx)
}
