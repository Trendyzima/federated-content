package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	AppEnv              string
	HTTPAddr            string
	PublicBaseURL       string
	InstanceDomain      string
	ServiceActorUsername string
	ServiceActorName    string
	ServiceActorSummary string
	PublicKeyPEM        string
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
	if err != nil || u.Host == "" || (u.Scheme != "https" && !(cfg.AppEnv != "production" && u.Scheme == "http")) {
		return Config{}, fmt.Errorf("PUBLIC_BASE_URL must be an absolute HTTPS URL (HTTP is allowed outside production)")
	}
	if strings.ContainsAny(cfg.ServiceActorUsername, "@/ ") {
		return Config{}, fmt.Errorf("SERVICE_ACTOR_USERNAME must be a simple username")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
