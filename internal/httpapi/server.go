package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/Trendyzima/federated-content/internal/backend"
	"github.com/Trendyzima/federated-content/internal/config"
	"github.com/Trendyzima/federated-content/internal/store"
)

const activityJSON = "application/activity+json"
const ldJSON = "application/ld+json"

type Server struct {
	cfg    config.Config
	logger *slog.Logger
	mux    *http.ServeMux
	state  *store.Store
	backend *backend.Client
}

func New(cfg config.Config, logger *slog.Logger) http.Handler {
	return NewWithRuntime(cfg, logger, nil, nil)
}

func NewWithRuntime(cfg config.Config, logger *slog.Logger, state *store.Store, backendClient *backend.Client) http.Handler {
	if logger == nil { logger = slog.Default() }
	s := &Server{cfg: cfg, logger: logger, mux: http.NewServeMux(), state: state, backend: backendClient}
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

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.state == nil || s.backend == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status":"preview_only","reason":"durable Federation storage and private Backend integration are not configured"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	if err := s.state.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status":"not_ready","reason":"federation storage is unavailable"})
		return
	}
	if err := s.backend.Ready(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status":"not_ready","reason":"private Backend dependency is unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status":"ready","storage":"ready","backend":"ready"})
}

func (s *Server) webfinger(w http.ResponseWriter, r *http.Request) {
	resource := strings.TrimSpace(r.URL.Query().Get("resource"))
	username, domain, ok := parseLocalAcct(resource)
	if !ok || domain != strings.ToLower(s.cfg.InstanceDomain) {
		writeError(w, http.StatusNotFound, "account_not_found", "The requested account is not hosted by this service")
		return
	}
	actorURL := s.actorURL()
	if username != s.cfg.ServiceActorUsername {
		if s.backend == nil || s.state == nil {
			writeError(w, http.StatusServiceUnavailable, "federation_identity_unavailable", "Local actor discovery is temporarily unavailable")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		profile, err := s.backend.ResolveActor(ctx, username)
		if errors.Is(err, backend.ErrActorNotFound) {
			writeError(w, http.StatusNotFound, "account_not_found", "The requested account is not hosted by this service")
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "federation_identity_unavailable", "Local actor discovery is temporarily unavailable")
			return
		}
		actorURL = s.cfg.PublicBaseURL + "/users/" + url.PathEscape(profile.UserID)
		if _, err := s.state.EnsureActor(ctx, profile.UserID, profile.Handle, actorURL, profile.DisplayName, profile.Summary); err != nil {
			writeError(w, http.StatusServiceUnavailable, "federation_actor_unavailable", "Local actor could not be provisioned")
			return
		}
	}
	subject := "acct:" + username + "@" + s.cfg.InstanceDomain
	w.Header().Set("Content-Type", "application/jrd+json; charset=utf-8")
	writeJSON(w, http.StatusOK, map[string]any{
		"subject": subject,
		"aliases": []string{actorURL},
		"links": []map[string]string{{"rel":"self","type":activityJSON,"href":actorURL}},
	})
}

func (s *Server) actor(w http.ResponseWriter, r *http.Request) {
	username := r.PathValue("username")
	if !acceptsActivityJSON(r.Header.Get("Accept")) {
		w.Header().Set("Vary", "Accept")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotAcceptable)
		_, _ = w.Write([]byte("Request an ActivityPub representation using Accept: application/activity+json"))
		return
	}
	if username == s.cfg.ServiceActorUsername {
		actor := map[string]any{
			"@context": []any{"https://www.w3.org/ns/activitystreams","https://w3id.org/security/v1"},
			"id": s.actorURL(), "type":"Application", "preferredUsername":s.cfg.ServiceActorUsername,
			"name":s.cfg.ServiceActorName, "summary":s.cfg.ServiceActorSummary,
			"inbox":s.actorURL()+"/inbox", "outbox":s.actorURL()+"/outbox", "url":s.cfg.PublicBaseURL,
		}
		if s.cfg.PublicKeyPEM!="" { actor["publicKey"]=map[string]string{"id":s.actorURL()+"#main-key","owner":s.actorURL(),"publicKeyPem":s.cfg.PublicKeyPEM} }
		w.Header().Set("Vary","Accept"); w.Header().Set("Cache-Control","public, max-age=300")
		writeActivityJSON(w,http.StatusOK,actor)
		return
	}
	if s.state==nil { writeError(w,http.StatusServiceUnavailable,"federation storage is unavailable"); return }
	actorURI:=s.cfg.PublicBaseURL+"/users/"+url.PathEscape(username)
	actor,err:=s.state.ActorByURI(r.Context(),actorURI)
	if errors.Is(err,sql.ErrNoRows) { writeError(w,http.StatusNotFound,"actor_not_found","Actor not found"); return }
	if err!=nil { writeError(w,http.StatusServiceUnavailable,"actor_unavailable","Actor lookup is unavailable"); return }
	actorDocument:=map[string]any{
		"@context":[]any{"https://www.w3.org/ns/activitystreams","https://w3id.org/security/v1"},
		"id":actor.ActorURI,"type":"Person","preferredUsername":actor.Username,
		"name":actor.DisplayName,"summary":actor.Summary,
		"inbox":actor.ActorURI+"/inbox","outbox":actor.ActorURI+"/outbox",
		"url":s.cfg.PublicBaseURL+"/@"+actor.Username,
		"publicKey":map[string]string{"id":actor.PublicKeyID,"owner":actor.ActorURI,"publicKeyPem":actor.PublicKeyPEM},
	}
	w.Header().Set("Vary","Accept"); w.Header().Set("Cache-Control","public, max-age=300")
	writeActivityJSON(w,http.StatusOK,actorDocument)
}

func (s *Server) outbox(w http.ResponseWriter, r *http.Request) {
	username:=r.PathValue("username")
	if !acceptsActivityJSON(r.Header.Get("Accept")) { http.Error(w,"ActivityPub Accept header required",http.StatusNotAcceptable); return }
	if username==s.cfg.ServiceActorUsername {
		writeActivityJSON(w,http.StatusOK,map[string]any{"@context":"https://www.w3.org/ns/activitystreams","id":s.actorURL()+"/outbox","type":"OrderedCollection","totalItems":0,"orderedItems":[]any{}})
		return
	}
	if s.state==nil { writeError(w,http.StatusServiceUnavailable,"federation storage is unavailable"); return }
	actorURI:=s.cfg.PublicBaseURL+"/users/"+url.PathEscape(username)
	actor,err:=s.state.ActorByURI(r.Context(),actorURI)
	if errors.Is(err,sql.ErrNoRows) { writeError(w,http.StatusNotFound,"actor_not_found","Actor not found"); return }
	if err!=nil { writeError(w,http.StatusServiceUnavailable,"outbox_unavailable","Outbox is unavailable"); return }
	items,total,err:=s.state.ListOutbox(r.Context(),actor.ActorURI,40)
	if err!=nil { writeError(w,http.StatusServiceUnavailable,"outbox_unavailable","Outbox is unavailable"); return }
	writeActivityJSON(w,http.StatusOK,map[string]any{"@context":"https://www.w3.org/ns/activitystreams","id":actor.ActorURI+"/outbox","type":"OrderedCollection","totalItems":total,"orderedItems":items})
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

func parseLocalAcct(resource string) (string,string,bool) {
	if !strings.HasPrefix(strings.ToLower(resource),"acct:") { return "","",false }
	value:=strings.TrimSpace(resource[5:])
	at:=strings.LastIndex(value,"@")
	if at<1||at==len(value)-1 { return "","",false }
	username:=strings.ToLower(value[:at])
	domain:=strings.ToLower(strings.TrimSuffix(value[at+1:],"." ))
	if !config.ValidActorUsername(username) { return "","",false }
	return username,domain,true
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
