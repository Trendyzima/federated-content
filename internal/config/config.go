package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

var actorUsernamePattern = regexp.MustCompile(`^[a-z0-9_][a-z0-9_.-]{0,63}$`)

type Config struct {
	AppEnv               string
	HTTPAddr             string
	PublicBaseURL        string
	InstanceDomain       string
	ServiceActorUsername string
	ServiceActorName     string
	ServiceActorSummary  string
	PublicKeyPEM         string
}

func Load() (Config, error) {
	cfg := Config{
		AppEnv:               env("APP_ENV", "development"),
		HTTPAddr:             env("HTTP_ADDR", ":8081"),
		PublicBaseURL:        strings.TrimRight(env("PUBLIC_BASE_URL", "https://federation.testagram.site"), "/"),
		InstanceDomain:       strings.ToLower(strings.TrimSpace(env("INSTANCE_DOMAIN", "testagram.site"))),
		ServiceActorUsername: strings.ToLower(strings.TrimSpace(env("SERVICE_ACTOR_USERNAME", "testagram"))),
		ServiceActorName:     env("SERVICE_ACTOR_NAME", "Testagram"),
		ServiceActorSummary:  env("SERVICE_ACTOR_SUMMARY", "The Testagram federated social network"),
		PublicKeyPEM:         strings.TrimSpace(os.Getenv("PUBLIC_KEY_PEM")),
	}
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
	return cfg, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
