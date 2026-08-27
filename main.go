package main

import (
	"context"
	"esAdb/common"
	"esAdb/config"
	"esAdb/model"
	"esAdb/router"
	"esAdb/store"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfgPath := flag.String("c", "config.yaml", "配置文件路径")
	flag.Parse()

	cfg, _ := config.Load(*cfgPath)

	// 日志级别：ESADB_LOG_LEVEL / config log.level = debug|info|warn|error|off
	common.SetLevel(cfg.Log.Level)

	if cfg.HasMySQL() && cfg.MySQL.Table != "" {
		model.SetTableName(cfg.MySQL.Table)
	}

	if !cfg.Ready {
		tip := cfg.Tip
		if tip == "" {
			tip = "无配置运行中，不会做任何操作"
		}
		common.Warn("%s", tip)
	} else {
		common.Info("配置已加载 es=%v mysql=%v field=%s dateField=%s table=%s interval=%ds",
			cfg.HasES(), cfg.HasMySQL(),
			cfg.ES.ParseField(), cfg.ES.DateField,
			model.GetTableName(),
			cfg.Sync.Interval)
	}

	mgr := store.Init(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mgr.StartIncremental(ctx)

	addr := cfg.Server.Addr
	common.Info("HTTP 监听 %s  →  GET /health | POST /sync/range | GET /sync/status (SSE 5s)", addr)

	srv := &http.Server{Addr: addr, Handler: router.New(cfg)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			common.Error("HTTP 退出: %v", err)
			os.Exit(1)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	common.Info("正在关闭...")
	cancel()
	_ = srv.Close()
}
