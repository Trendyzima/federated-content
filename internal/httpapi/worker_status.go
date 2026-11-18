package httpapi

import (
	"net/http"
)

func (s *Server) internalWorkerStatus(w http.ResponseWriter, r *http.Request) {
	if s.cfg.FederationIngressToken == "" || !validInternalBearer(r, s.cfg.FederationIngressToken) {
		writeError(w, http.StatusUnauthorized, "internal_auth_required", "A valid Backend service credential is required")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "configured",
		"workers": map[string]bool{
			"delivery_worker": s.cfg.DeliveryWorkerEnabled,
			"outbox_poller":   s.cfg.OutboxPollerEnabled,
			"inbox_worker":   s.cfg.InboxWorkerEnabled,
			"inbox_accept":   s.cfg.InboxAcceptEnabled,
		},
	})
}
