package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Trendyzima/federated-content/internal/config"
)

const activityJSON = "application/activity+json"
const ldJSON = "application/ld+json"

type Server struct {
	cfg    config.Config
	logger *slog.Logger
	mux    *http.ServeMux
}

func New(cfg config.Config, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s.recoverer(s.requestLog(s.securityHeaders(s.mux)))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.health)
	s.mux.HandleFunc("GET /readyz", s.ready)
	s.mux.HandleFunc("GET /.well-known/webfinger", s.webfinger)
	s.mux.HandleFunc("GET /users/{username}", s.actor)
	s.mux.HandleFunc("GET /users/{username}/outbox", s.outbox)
	s.mux.HandleFunc("POST /users/{username}/inbox", s.inbox)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	// Keep the service out of production routing until it can safely accept and
	// persist federation traffic. Liveness remains healthy so the process can
	// be inspected during preview, but readiness must not imply protocol safety.
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"status": "preview_only",
		"reason": "HTTP Signature verification and durable inbox/outbox processing are not enabled",
	})
}

func (s *Server) webfinger(w http.ResponseWriter, r *http.Request) {
	resource := strings.TrimSpace(r.URL.Query().Get("resource"))
	expected := "acct:" + s.cfg.ServiceActorUsername + "@" + s.cfg.InstanceDomain
	if resource != expected {
		writeError(w, http.StatusNotFound, "account_not_found", "The requested account is not hosted by this service")
		return
	}
	actorURL := s.actorURL()
	w.Header().Set("Content-Type", "application/jrd+json; charset=utf-8")
	writeJSON(w, http.StatusOK, map[string]any{
		"subject": expected,
		"aliases": []string{actorURL},
		"links": []map[string]string{{
			"rel": "self", "type": activityJSON, "href": actorURL,
		}},
	})
}

func (s *Server) actor(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("username") != s.cfg.ServiceActorUsername {
		writeError(w, http.StatusNotFound, "actor_not_found", "Actor not found")
		return
	}
	if !acceptsActivityJSON(r.Header.Get("Accept")) {
		w.Header().Set("Vary", "Accept")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotAcceptable)
		_, _ = w.Write([]byte("Request an ActivityPub representation using Accept: application/activity+json"))
		return
	}

	actor := map[string]any{
		"@context": []any{
			"https://www.w3.org/ns/activitystreams",
			"https://w3id.org/security/v1",
		},
		"id":                s.actorURL(),
		"type":              "Application",
		"preferredUsername": s.cfg.ServiceActorUsername,
		"name":              s.cfg.ServiceActorName,
		"summary":           s.cfg.ServiceActorSummary,
		"inbox":             s.actorURL() + "/inbox",
		"outbox":            s.actorURL() + "/outbox",
		"url":               s.cfg.PublicBaseURL,
	}
	if s.cfg.PublicKeyPEM != "" {
		actor["publicKey"] = map[string]string{
			"id":           s.actorURL() + "#main-key",
			"owner":        s.actorURL(),
			"publicKeyPem": s.cfg.PublicKeyPEM,
		}
	}
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Cache-Control", "public, max-age=300")
	writeActivityJSON(w, http.StatusOK, actor)
}

func (s *Server) outbox(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("username") != s.cfg.ServiceActorUsername {
		writeError(w, http.StatusNotFound, "actor_not_found", "Actor not found")
		return
	}
	if !acceptsActivityJSON(r.Header.Get("Accept")) {
		http.Error(w, "ActivityPub Accept header required", http.StatusNotAcceptable)
		return
	}
	writeActivityJSON(w, http.StatusOK, map[string]any{
		"@context":     "https://www.w3.org/ns/activitystreams",
		"id":           s.actorURL() + "/outbox",
		"type":         "OrderedCollection",
		"totalItems":   0,
		"orderedItems": []any{},
	})
}

func (s *Server) inbox(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("username") != s.cfg.ServiceActorUsername {
		writeError(w, http.StatusNotFound, "actor_not_found", "Actor not found")
		return
	}
	// Fail closed: accepting unsigned or unverified activities would permit
	// spoofing, replay, spam, and arbitrary side effects.
	http.Error(w, "Inbox processing is not enabled until HTTP Signature verification is configured", http.StatusNotImplemented)
}

func (s *Server) actorURL() string {
	return s.cfg.PublicBaseURL + "/users/" + s.cfg.ServiceActorUsername
}

func acceptsActivityJSON(header string) bool {
	for _, part := range strings.Split(header, ",") {
		mediaType := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		if mediaType == activityJSON || mediaType == ldJSON || mediaType == "*/*" {
			return true
		}
	}
	return false
}

func writeActivityJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", activityJSON+"; charset=utf-8")
	writeJSON(w, status, value)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("HTTP handler panic", "panic", recovered)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
