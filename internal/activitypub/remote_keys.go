package activitypub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type RemoteKeyResolver struct {
	client *http.Client
}

func NewRemoteKeyResolver() *RemoteKeyResolver {
	transport := &http.Transport{
		DialContext:           safePublicDialContext,
		MaxIdleConns:          20,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
	}
	return &RemoteKeyResolver{client: &http.Client{
		Transport: transport,
		Timeout:   7 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (r *RemoteKeyResolver) Resolve(ctx context.Context, keyID string) (PublicKey, error) {
	u, err := url.Parse(keyID)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment == "" {
		return PublicKey{}, errors.New("remote key ID must be an HTTPS URL with a fragment")
	}
	fragment := u.Fragment
	u.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return PublicKey{}, err
	}
	req.Header.Set("Accept", "application/activity+json, application/ld+json")
	resp, err := r.client.Do(req)
	if err != nil {
		return PublicKey{}, fmt.Errorf("fetch remote actor: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return PublicKey{}, fmt.Errorf("remote actor returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return PublicKey{}, errors.New("remote actor document is invalid or too large")
	}
	var actor struct {
		ID        string `json:"id"`
		Inbox     string `json:"inbox"`
		Endpoints struct {
			SharedInbox string `json:"sharedInbox"`
		} `json:"endpoints"`
		PublicKey struct {
			ID    string `json:"id"`
			Owner string `json:"owner"`
			PEM   string `json:"publicKeyPem"`
		} `json:"publicKey"`
	}
	if err := json.Unmarshal(body, &actor); err != nil {
		return PublicKey{}, errors.New("remote actor document is invalid")
	}
	if actor.ID != u.String() || actor.PublicKey.ID != keyID || actor.PublicKey.Owner != actor.ID || actor.PublicKey.PEM == "" || fragment == "" {
		return PublicKey{}, errors.New("remote actor public key identity does not match keyId")
	}
	if err := validateRemoteHTTPSURL(actor.Inbox); err != nil {
		return PublicKey{}, errors.New("remote actor inbox URL is invalid")
	}
	if actor.Endpoints.SharedInbox != "" {
		if err := validateRemoteHTTPSURL(actor.Endpoints.SharedInbox); err != nil {
			return PublicKey{}, errors.New("remote actor shared inbox URL is invalid")
		}
	}
	return PublicKey{ID: actor.PublicKey.ID, Owner: actor.ID, PEM: actor.PublicKey.PEM, Inbox: actor.Inbox, SharedInbox: actor.Endpoints.SharedInbox}, nil
}

func validateRemoteHTTPSURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
		return errors.New("remote URL must be HTTPS")
	}
	return nil
}

func safePublicDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if port != "443" {
		return nil, errors.New("remote ActivityPub HTTPS port must be 443")
	}
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolved, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, entry := range resolved {
			ips = append(ips, entry.IP)
		}
	}
	var lastErr error
	for _, ip := range ips {
		if !isPublicIP(ip) {
			continue
		}
		conn, err := (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.String(), strconv.Itoa(443)))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errors.New("remote host did not resolve to a public IP address")
}

var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
}

func isPublicIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

func sameActorURI(a, b string) bool {
	return strings.TrimRight(a, "/") == strings.TrimRight(b, "/")
}
