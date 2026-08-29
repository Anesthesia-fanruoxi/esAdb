package router

import (
	"net/http"

	"esAdb/api"
	"esAdb/common"
	"esAdb/config"
	"esAdb/store"
)

// New 注册路由
func New(cfg *config.Config, mgr *store.Manager) http.Handler {
	mux := http.NewServeMux()
	syncAPI := api.NewSyncAPI(cfg, mgr)
	monitorAPI := api.NewMonitorAPI(mgr)

	mux.HandleFunc("/", monitorAPI.HandleIndex)
	mux.HandleFunc("/monitor/sse", monitorAPI.HandleSSE)
	mux.HandleFunc("/monitor/backfill/sse", monitorAPI.HandleBackfillSSE)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		common.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/sync/backfill", syncAPI.HandleBackfill)
	mux.HandleFunc("/sync/backfill/windows", syncAPI.HandleBackfillWindows)
	mux.HandleFunc("/sync/compare", syncAPI.HandleCompare)
	mux.HandleFunc("/sync/compare/drilldown/sse", syncAPI.HandleCompareDrilldownSSE)

	return mux
}
