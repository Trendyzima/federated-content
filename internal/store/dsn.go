package store

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

func validateDatabaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() == "" || u.User == nil || u.Path == "" {
		return "", errors.New("FEDERATION_DATABASE_URL must be a PostgreSQL URL with host, database and credentials")
	}
	if u.Fragment != "" {
		return "", errors.New("FEDERATION_DATABASE_URL must not contain a fragment")
	}
	query := u.Query()
	sslmode := strings.ToLower(strings.TrimSpace(query.Get("sslmode")))
	if isLocalDatabaseHost(u.Hostname()) {
		if sslmode == "" {
			query.Set("sslmode", "disable")
		}
	} else if sslmode != "require" && sslmode != "verify-ca" && sslmode != "verify-full" {
		return "", errors.New("remote FEDERATION_DATABASE_URL must set sslmode=require or stronger")
	}
	if timeout := query.Get("connect_timeout"); timeout != "" {
		if n, err := strconv.Atoi(timeout); err != nil || n < 1 || n > 30 {
			return "", errors.New("FEDERATION_DATABASE_URL connect_timeout must be between 1 and 30 seconds")
		}
	} else {
		query.Set("connect_timeout", "5")
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func isLocalDatabaseHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "federation-db"
}
