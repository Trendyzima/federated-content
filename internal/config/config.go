package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var actorUsernamePattern = regexp.MustCompile(`^[a-z0-9_][a-z0-9_.-]{0,63}$`)

type Config struct {
	AppEnv                string
	HTTPAddr              string
	PublicBaseURL         string
	InstanceDomain        string
	ServiceActorUsername  string
	ServiceActorName      string
	ServiceActorSummary   string
	PublicKeyPEM          string
	FederationDatabaseURL string
	BackendInternalURL    string
	BackendInternalToken  string
	DeliveryWorkerEnabled bool
	OutboxPollerEnabled   bool
	InboxWorkerEnabled    bool
}

func Load() (Config, error) {
	cfg := Config{
		AppEnv:                env("APP_ENV", "development"),
		HTTPAddr:              env("HTTP_ADDR", ":8081"),
		PublicBaseURL:         strings.TrimRight(env("PUBLIC_BASE_URL", "https://federation.testagram.site"), "/"),
		InstanceDomain:        strings.ToLower(strings.TrimSpace(env("INSTANCE_DOMAIN", "testagram.site"))),
		ServiceActorUsername:  strings.ToLower(strings.TrimSpace(env("SERVICE_ACTOR_USERNAME", "testagram"))),
		ServiceActorName:      env("SERVICE_ACTOR_NAME", "Testagram"),
		ServiceActorSummary:   env("SERVICE_ACTOR_SUMMARY", "The Testagram federated social network"),
		PublicKeyPEM:          strings.TrimSpace(os.Getenv("PUBLIC_KEY_PEM")),
		FederationDatabaseURL: strings.TrimSpace(os.Getenv("FEDERATION_DATABASE_URL")),
		BackendInternalURL:    strings.TrimRight(strings.TrimSpace(os.Getenv("BACKEND_INTERNAL_URL")), "/"),
		BackendInternalToken:  strings.TrimSpace(os.Getenv("BACKEND_INTERNAL_TOKEN")),
	}
	deliveryWorkerEnabled, err := strconv.ParseBool(env("FEDERATION_DELIVERY_WORKER_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("FEDERATION_DELIVERY_WORKER_ENABLED must be true or false")
	}
	cfg.DeliveryWorkerEnabled = deliveryWorkerEnabled
	outboxPollerEnabled, err := strconv.ParseBool(env("FEDERATION_OUTBOX_POLLER_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("FEDERATION_OUTBOX_POLLER_ENABLED must be true or false")
	}
	cfg.OutboxPollerEnabled = outboxPollerEnabled
	inboxWorkerEnabled, err := strconv.ParseBool(env("FEDERATION_INBOX_WORKER_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("FEDERATION_INBOX_WORKER_ENABLED must be true or false")
	}
	cfg.InboxWorkerEnabled = inboxWorkerEnabled
	if cfg.HTTPAddr == "" || cfg.InstanceDomain == "" || cfg.ServiceActorUsername == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR, INSTANCE_DOMAIN and SERVICE_ACTOR_USERNAME must not be empty")
	}
	u, err := url.ParseRequestURI(cfg.PublicBaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return Config{}, fmt.Errorf("PUBLIC_BASE_URL must be an absolute origin URL without credentials, path, query or fragment")
	}
	if u.Scheme != "https" && !((cfg.AppEnv == "development" || cfg.AppEnv == "test") && u.Scheme == "http") {
		return Config{}, fmt.Errorf("PUBLIC_BASE_URL must use HTTPS (HTTP is allowed only in development and test)")
	}
	hostname := strings.ToLower(u.Hostname())
	if hostname == "" || (hostname != cfg.InstanceDomain && !strings.HasSuffix(hostname, "."+cfg.InstanceDomain)) {
		return Config{}, fmt.Errorf("PUBLIC_BASE_URL host must be INSTANCE_DOMAIN or one of its subdomains")
	}
	if !actorUsernamePattern.MatchString(cfg.ServiceActorUsername) || cfg.ServiceActorUsername == "." || cfg.ServiceActorUsername == ".." {
		return Config{}, fmt.Errorf("SERVICE_ACTOR_USERNAME must be 1-64 lowercase letters, digits, underscores, dots or hyphens and start with a letter, digit or underscore")
	}
	if cfg.InboxWorkerEnabled && !cfg.DeliveryWorkerEnabled {
		return Config{}, fmt.Errorf("FEDERATION_INBOX_WORKER_ENABLED requires FEDERATION_DELIVERY_WORKER_ENABLED")
	}
	if cfg.InboxWorkerEnabled && cfg.FederationDatabaseURL == "" {
		return Config{}, fmt.Errorf("FEDERATION_INBOX_WORKER_ENABLED requires FEDERATION_DATABASE_URL")
	}
	if (cfg.BackendInternalURL == "") != (cfg.BackendInternalToken == "") {
		return Config{}, fmt.Errorf("BACKEND_INTERNAL_URL and BACKEND_INTERNAL_TOKEN must be configured together")
	}
	if cfg.InboxWorkerEnabled && cfg.BackendInternalURL == "" {
		return Config{}, fmt.Errorf("FEDERATION_INBOX_WORKER_ENABLED requires private Backend integration")
	}
	if cfg.BackendInternalURL != "" {
		backendURL, backendErr := url.Parse(cfg.BackendInternalURL)
		if backendErr != nil || backendURL.Host == "" || backendURL.User != nil || backendURL.RawQuery != "" || backendURL.Fragment != "" ||
			(backendURL.Scheme != "https" && !(backendURL.Scheme == "http" && (backendURL.Hostname() == "api" || backendURL.Hostname() == "localhost" || backendURL.Hostname() == "127.0.0.1"))) {
			return Config{}, fmt.Errorf("BACKEND_INTERNAL_URL must be HTTPS (HTTP only for the private api or localhost service)")
		}
		if len(cfg.BackendInternalToken) < 32 || strings.ContainsAny(cfg.BackendInternalToken, "\r\n") {
			return Config{}, fmt.Errorf("BACKEND_INTERNAL_TOKEN must contain at least 32 characters")
		}
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func ValidActorUsername(value string) bool {
	return actorUsernamePattern.MatchString(strings.ToLower(strings.TrimSpace(value)))
}
