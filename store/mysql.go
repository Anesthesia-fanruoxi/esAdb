package store

import (
	"fmt"
	"strings"

	"esAdb/common"
	"esAdb/config"
	"esAdb/model"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// MySQLStore 阿里云 ADB / MySQL 数据访问（sqlx，手写 SQL）
type MySQLStore struct {
	db    *sqlx.DB
	table string
}

// NewMySQLStore 连接数据库并确保表结构就绪
func NewMySQLStore(cfg config.MySQLConfig) (*MySQLStore, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, fmt.Errorf("MySQL 未配置")
	}
	if cfg.Table != "" {
		model.SetTableName(cfg.Table)
	}

	db, err := sqlx.Connect("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)

	s := &MySQLStore{db: db, table: model.GetTableName()}
	if err := s.ensureSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// ensureSchema 表不存在则创建，已存在则只补缺失列
func (s *MySQLStore) ensureSchema() error {
	exists, err := s.tableExists()
	if err != nil {
		return err
	}
	if !exists {
		common.Info("表 %s 不存在，自动创建", s.table)
		if err := s.createTable(); err != nil {
			return fmt.Errorf("自动创建表失败: %w", err)
		}
		common.Info("表 %s 创建成功", s.table)
		return nil
	}
	if err := s.ensureColumns(); err != nil {
		return err
	}
	common.Info("MySQL 已连接，表 %s 已存在", s.table)
	return nil
}

func (s *MySQLStore) tableExists() (bool, error) {
	var cnt int
	err := s.db.Get(&cnt, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE() AND table_name = ?`, s.table)
	return cnt > 0, err
}

// createTable 显式建表，避免 ORM 自动迁移在 ADB 上反复 ALTER text 列
func (s *MySQLStore) createTable() error {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(64) NOT NULL PRIMARY KEY,
		type VARCHAR(16) DEFAULT NULL,
		phone_md5 VARCHAR(64) DEFAULT NULL,
		customer_id VARCHAR(32) DEFAULT NULL,
		event_id VARCHAR(32) DEFAULT NULL,
		event_name VARCHAR(128) DEFAULT NULL,
		channel_id VARCHAR(32) DEFAULT NULL,
		channel_name VARCHAR(128) DEFAULT NULL,
		client VARCHAR(32) DEFAULT NULL,
		path VARCHAR(64) DEFAULT NULL,
		ip VARCHAR(64) DEFAULT NULL,
		create_time DATETIME DEFAULT NULL,
		source TEXT,
		app_version_type VARCHAR(64) DEFAULT NULL,
		extend TEXT,
		es_timestamp BIGINT DEFAULT NULL,
		KEY idx_es_timestamp (es_timestamp)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, quoteTable(s.table))
	_, err := s.db.Exec(sql)
	return err
}

// ensureColumns 仅 ADD COLUMN，不做 MODIFY
func (s *MySQLStore) ensureColumns() error {
	need := map[string]string{
		"type":             "VARCHAR(16) DEFAULT NULL",
		"phone_md5":        "VARCHAR(64) DEFAULT NULL",
		"customer_id":      "VARCHAR(32) DEFAULT NULL",
		"event_id":         "VARCHAR(32) DEFAULT NULL",
		"event_name":       "VARCHAR(128) DEFAULT NULL",
		"channel_id":       "VARCHAR(32) DEFAULT NULL",
		"channel_name":     "VARCHAR(128) DEFAULT NULL",
		"client":           "VARCHAR(32) DEFAULT NULL",
		"path":             "VARCHAR(64) DEFAULT NULL",
		"ip":               "VARCHAR(64) DEFAULT NULL",
		"create_time":      "DATETIME DEFAULT NULL",
		"source":           "TEXT",
		"app_version_type": "VARCHAR(64) DEFAULT NULL",
		"extend":           "TEXT",
		"es_timestamp":     "BIGINT DEFAULT NULL",
	}
	for col, def := range need {
		ok, err := s.columnExists(col)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		common.Info("表缺少列 %s，自动添加", col)
		_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteTable(s.table), quoteCol(col), def))
		if err != nil {
			common.Warn("添加列 %s 失败: %v", col, err)
		}
	}
	return nil
}

func (s *MySQLStore) columnExists(col string) (bool, error) {
	var cnt int
	err := s.db.Get(&cnt, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		s.table, col)
	return cnt > 0, err
}

func quoteTable(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}

func quoteCol(name string) string {
	return quoteTable(name)
}

// BatchInsertIgnore 批量写入（ADB 使用 REPLACE INTO）
func (s *MySQLStore) BatchInsertIgnore(logs []*model.EventLog) (written int, replacedHint int, err error) {
	if s == nil || s.db == nil {
		return 0, 0, fmt.Errorf("MySQL 未初始化")
	}
	if len(logs) == 0 {
		return 0, 0, nil
	}

	cols := strings.Join(model.Columns, ", ")
	oneRow := "(" + strings.TrimRight(strings.Repeat("?,", len(model.Columns)), ",") + ")"

	const chunk = 200
	for i := 0; i < len(logs); i += chunk {
		end := i + chunk
		if end > len(logs) {
			end = len(logs)
		}
		part := logs[i:end]

		placeholders := make([]string, len(part))
		args := make([]interface{}, 0, len(part)*len(model.Columns))
		for j, el := range part {
			placeholders[j] = oneRow
			args = append(args,
				el.ID, el.Type, el.PhoneMd5, el.CustomerID, el.EventID, el.EventName,
				el.ChannelID, el.ChannelName, el.Client, el.Path, el.IP, el.CreateTime,
				el.Source, el.AppVersionType, el.Extend, el.EsTimestamp,
			)
		}

		sql := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
			quoteTable(s.table), cols, strings.Join(placeholders, ","))

		res, err := s.db.Exec(sql, args...)
		if err != nil {
			return written, replacedHint, err
		}
		written += len(part)
		if aff, e := res.RowsAffected(); e == nil {
			replacedHint += int(aff)
		}
	}
	return written, replacedHint, nil
}

// BuildFromESRecords 解析 ES 记录为模型（带 es_timestamp 毫秒）
func BuildFromESRecords(records []ESRecord) []*model.EventLog {
	var logs []*model.EventLog
	for _, r := range records {
		m := common.ConvertMap(r.Content)
		if m["id"] == "" {
			continue
		}
		el, err := common.MapToEventLog(m)
		if err != nil {
			common.Warn("跳过一条: %v", err)
			continue
		}
		el.EsTimestamp = r.EsTimestamp
		logs = append(logs, el)
	}
	return logs
}
