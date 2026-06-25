package api

import (
	"net/http"

	"go_distributed_system/internal/presets"
)

func (s *Server) handlePresets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	items := presets.List()
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, map[string]any{
			"id":          p.ID,
			"description": p.Description,
			"output_exts": p.OutputExts,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"presets": out})
}
