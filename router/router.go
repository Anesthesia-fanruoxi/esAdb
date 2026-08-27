package router

import (
	"esAdb/api"
	"esAdb/common"
	"esAdb/config"
	"net/http"
)

// New 注册路由
func New(cfg *config.Config) http.Handler {
	mux := http.NewServeMux()
	syncAPI := api.NewSyncAPI(cfg)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		common.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/sync/range", syncAPI.HandleRange)
	mux.HandleFunc("/sync/status", syncAPI.HandleStatus)

	return mux
}
