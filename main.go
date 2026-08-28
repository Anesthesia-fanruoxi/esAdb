package main

import (
	"context"
	"fmt"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"esAdb/common"
	"esAdb/config"
	"esAdb/model"
	"esAdb/router"
	"esAdb/store"
)

func main() {
	cfgPath := flag.String("c", "config/config.yaml", "配置文件路径（容器内默认 /config/config.yaml）")
	port := flag.Int("p", 0, "HTTP 监听端口，覆盖配置 server.addr（也可用 -port）")
	flag.IntVar(port, "port", 0, "HTTP 监听端口，同 -p")
	flag.Parse()

	cfg, _ := config.Load(*cfgPath)

	// 启动诊断：即使日志级别为 off，配置异常也输出到 stderr
	if cfg.Tip != "" {
		fmt.Fprintf(os.Stderr, "[es-adb] 配置路径: %s | %s\n", *cfgPath, cfg.Tip)
	}

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
	if *port > 0 {
		addr = fmt.Sprintf(":%d", *port)
		common.Info("命令行指定端口 port=%d，覆盖配置 addr=%s", *port, cfg.Server.Addr)
	}
	common.Info("HTTP 监听 %s  →  GET / | /monitor/sse | /health | /sync/backfill | /sync/compare", addr)

	srv := &http.Server{Addr: addr, Handler: router.New(cfg, mgr)}
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
