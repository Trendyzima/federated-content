package activitypub

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type PublicKey struct {
	ID          string
	Owner       string
	PEM         string
	Inbox       string
	SharedInbox string
}

type KeyResolver interface {
	Resolve(ctx context.Context, keyID string) (PublicKey, error)
}

func SignRequest(request *http.Request, body []byte, keyID, privateKeyPEM string, now time.Time) error {
	if request == nil || request.URL == nil || request.URL.Scheme != "https" || request.URL.Host == "" || request.URL.User != nil || request.URL.Fragment != "" {
		return errors.New("outbound ActivityPub request must target an HTTPS URL")
	}
	if len(body) == 0 || len(body) > 1<<20 || keyID == "" || len(keyID) > 2048 || strings.ContainsAny(keyID, "\\r\\n\\\"") || len(privateKeyPEM) > 16<<10 {
		return errors.New("outbound ActivityPub signature input is invalid")
	}
	key, err := parseRSAPrivateKey(privateKeyPEM)
	if err != nil {
		return err
	}
	request.Host = request.URL.Host
	request.Header.Set("Date", now.UTC().Format(http.TimeFormat))
	digest := sha256.Sum256(body)
	request.Header.Set("Digest", "SHA-256="+base64.StdEncoding.EncodeToString(digest[:]))
	request.Header.Set("Content-Type", "application/activity+json")
	request.Header.Set("Accept", "application/activity+json")
	headers := "(request-target) host date digest"
	canonical := strings.Join([]string{
		"(request-target): " + strings.ToLower(request.Method) + " " + request.URL.RequestURI(),
		"host: " + request.Host,
		"date: " + request.Header.Get("Date"),
		"digest: " + request.Header.Get("Digest"),
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return fmt.Errorf("sign outbound ActivityPub request: %w", err)
	}
	request.Header.Set("Signature", `keyId="`+keyID+`",algorithm="rsa-sha256",headers="`+headers+`",signature="`+base64.StdEncoding.EncodeToString(signature)+`"`)
	return nil
}

func parseRSAPrivateKey(value string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(value))
	if block == nil {
		return nil, errors.New("local private key PEM is invalid")
	}
	var parsed any
	var err error
	if block.Type == "RSA PRIVATE KEY" {
		parsed, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	} else {
		parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	}
	if err != nil {
		return nil, errors.New("local private key could not be parsed")
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok || key.N.BitLen() < 2048 || key.N.BitLen() > 8192 {
		return nil, errors.New("local RSA private key size is unsupported")
	}
	return key, nil
}

func VerifyRequest(ctx context.Context, request *http.Request, body []byte, resolver KeyResolver, now time.Time) (PublicKey, error) {
	if request == nil || resolver == nil {
		return PublicKey{}, errors.New("signature verifier is not configured")
	}
	if len(body) == 0 || len(body) > 1<<20 {
		return PublicKey{}, errors.New("ActivityPub body size is invalid")
	}
	params, err := parseSignatureHeader(request.Header.Get("Signature"))
	if err != nil {
		return PublicKey{}, err
	}
	if params["algorithm"] != "rsa-sha256" {
		return PublicKey{}, errors.New("only rsa-sha256 HTTP signatures are supported")
	}
	keyID := params["keyId"]
	if keyID == "" {
		return PublicKey{}, errors.New("signature keyId is required")
	}
	signedHeaders := strings.Fields(strings.ToLower(params["headers"]))
	required := map[string]bool{"(request-target)": false, "host": false, "date": false, "digest": false}
	for _, header := range signedHeaders {
		if _, ok := required[header]; ok {
			required[header] = true
		}
	}
	for header, present := range required {
		if !present {
			return PublicKey{}, fmt.Errorf("signature must cover %s", header)
		}
	}
	dateValue := request.Header.Get("Date")
	date, err := http.ParseTime(dateValue)
	if err != nil || now.Sub(date) > 5*time.Minute || date.Sub(now) > 5*time.Minute {
		return PublicKey{}, errors.New("HTTP Signature Date is missing or outside the five-minute window")
	}
	digestValue := request.Header.Get("Digest")
	if !verifyDigest(digestValue, body) {
		return PublicKey{}, errors.New("HTTP Signature Digest does not match request body")
	}
	key, err := resolver.Resolve(ctx, keyID)
	if err != nil {
		return PublicKey{}, fmt.Errorf("resolve remote signing key: %w", err)
	}
	if key.ID != keyID || key.Owner == "" || key.PEM == "" {
		return PublicKey{}, errors.New("resolved signing key identity is invalid")
	}
	publicKey, err := parseRSAPublicKey(key.PEM)
	if err != nil {
		return PublicKey{}, err
	}
	canonical := make([]string, 0, len(signedHeaders))
	for _, header := range signedHeaders {
		var value string
		switch header {
		case "(request-target)":
			value = strings.ToLower(request.Method) + " " + request.URL.RequestURI()
		case "host":
			value = request.Host
		default:
			value = request.Header.Get(http.CanonicalHeaderKey(header))
		}
		if value == "" {
			return PublicKey{}, fmt.Errorf("signed header %s is missing", header)
		}
		canonical = append(canonical, header+": "+value)
	}
	signature, err := base64.StdEncoding.DecodeString(params["signature"])
	if err != nil || len(signature) == 0 {
		return PublicKey{}, errors.New("HTTP Signature value is invalid")
	}
	sum := sha256.Sum256([]byte(strings.Join(canonical, "\n")))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, sum[:], signature); err != nil {
		return PublicKey{}, errors.New("HTTP Signature verification failed")
	}
	return key, nil
}

func verifyDigest(value string, body []byte) bool {
	for _, part := range strings.Split(value, ",") {
		algorithm, encoded, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || !strings.EqualFold(algorithm, "SHA-256") {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			continue
		}
		expected := sha256.Sum256(body)
		if len(got) == len(expected) {
			var diff byte
			for i := range got {
				diff |= got[i] ^ expected[i]
			}
			if diff == 0 {
				return true
			}
		}
	}
	return false
}

func parseRSAPublicKey(value string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(value))
	if block == nil {
		return nil, errors.New("remote public key PEM is invalid")
	}
	var parsed any
	var err error
	if block.Type == "RSA PUBLIC KEY" {
		parsed, err = x509.ParsePKCS1PublicKey(block.Bytes)
	} else {
		parsed, err = x509.ParsePKIXPublicKey(block.Bytes)
	}
	if err != nil {
		return nil, errors.New("remote public key could not be parsed")
	}
	key, ok := parsed.(*rsa.PublicKey)
	if !ok || key.N.BitLen() < 2048 || key.N.BitLen() > 8192 {
		return nil, errors.New("remote RSA public key size is unsupported")
	}
	return key, nil
}

func parseSignatureHeader(header string) (map[string]string, error) {
	header = strings.TrimSpace(header)
	if header == "" || len(header) > 16<<10 {
		return nil, errors.New("HTTP Signature header is missing or too large")
	}
	result := make(map[string]string)
	for len(header) > 0 {
		header = strings.TrimSpace(header)
		if header == "" {
			break
		}
		key, rest, ok := strings.Cut(header, "=")
		if !ok {
			return nil, errors.New("HTTP Signature parameter is malformed")
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("HTTP Signature parameter name is empty")
		}
		var value string
		if strings.HasPrefix(rest, "\"") {
			rest = rest[1:]
			end := strings.IndexByte(rest, '"')
			if end < 0 {
				return nil, errors.New("HTTP Signature quoted parameter is unterminated")
			}
			value = rest[:end]
			header = rest[end+1:]
		} else {
			value, header, _ = strings.Cut(rest, ",")
		}
		if _, exists := result[key]; exists {
			return nil, errors.New("duplicate HTTP Signature parameter")
		}
		result[key] = value
		header = strings.TrimSpace(header)
		if strings.HasPrefix(header, ",") {
			header = header[1:]
		}
	}
	if result["keyId"] == "" || result["signature"] == "" || result["headers"] == "" || result["algorithm"] == "" {
		return nil, errors.New("HTTP Signature is missing required parameters")
	}
	return result, nil
}
