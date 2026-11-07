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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

type fixedResolver struct {
	key PublicKey
	err error
}

func (f fixedResolver) Resolve(context.Context, string) (PublicKey, error) { return f.key, f.err }

func TestVerifyRequestAcceptsValidRSASignatureAndDigest(t *testing.T) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	owner := "https://remote.example/users/alice"
	keyID := owner + "#main-key"
	publicDER, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	body := []byte("{\"id\":\"https://remote.example/activities/1\",\"type\":\"Follow\",\"actor\":\"" + owner + "\"}")
	now := time.Now().UTC().Truncate(time.Second)
	req := httptest.NewRequest("POST", "https://federation.testagram.site/users/local_1/inbox", strings.NewReader(string(body)))
	req.Host = "federation.testagram.site"
	req.Header.Set("Date", now.Format(http.TimeFormat))
	digest := sha256.Sum256(body)
	req.Header.Set("Digest", "SHA-256="+base64.StdEncoding.EncodeToString(digest[:]))
	headers := "(request-target) host date digest"
	canonical := "(request-target): post " + req.URL.RequestURI() + "\nhost: " + req.Host + "\ndate: " + req.Header.Get("Date") + "\ndigest: " + req.Header.Get("Digest")
	sum := sha256.Sum256([]byte(canonical))
	signature, err := rsa.SignPKCS1v15(rand.Reader, private, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Signature", `keyId="`+keyID+`",algorithm="rsa-sha256",headers="`+headers+`",signature="`+base64.StdEncoding.EncodeToString(signature)+`"`)
	resolver := fixedResolver{key: PublicKey{ID: keyID, Owner: owner, PEM: publicPEM, Inbox: "https://remote.example/inbox"}}
	verified, err := VerifyRequest(context.Background(), req, body, resolver, now)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Owner != owner || verified.ID != keyID {
		t.Fatalf("unexpected verified key: %#v", verified)
	}
}

func TestSignRequestProducesVerifiableSignature(t *testing.T) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	owner := "https://federation.testagram.site/users/alice"
	keyID := owner + "#main-key"
	privatePEM := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(private)}))
	publicDER, err := x509.MarshalPKIXPublicKey(&private.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	body := []byte(`{"id":"https://federation.testagram.site/activities/1","type":"Create"}`)
	now := time.Now().UTC().Truncate(time.Second)
	req := httptest.NewRequest(http.MethodPost, "https://remote.example/inbox?shared=1", strings.NewReader(string(body)))
	if err := SignRequest(req, body, keyID, privatePEM, now); err != nil {
		t.Fatal(err)
	}
	if req.Header.Get("Date") != now.Format(http.TimeFormat) || !verifyDigest(req.Header.Get("Digest"), body) {
		t.Fatalf("signer emitted invalid Date or Digest headers: %#v", req.Header)
	}
	verified, err := VerifyRequest(context.Background(), req, body, fixedResolver{
		key: PublicKey{ID: keyID, Owner: owner, PEM: publicPEM},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if verified.ID != keyID {
		t.Fatalf("verified key ID=%q want=%q", verified.ID, keyID)
	}
}

func TestVerifyRequestRejectsTamperedDigestAndExpiredDate(t *testing.T) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	owner := "https://remote.example/users/alice"
	keyID := owner + "#main-key"
	publicDER, _ := x509.MarshalPKIXPublicKey(&private.PublicKey)
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	body := []byte("{\"id\":\"https://remote.example/a\",\"type\":\"Follow\",\"actor\":\"" + owner + "\"}")
	now := time.Now().UTC()
	req := httptest.NewRequest("POST", "https://federation.testagram.site/users/local/inbox", strings.NewReader(string(body)))
	req.Host = "federation.testagram.site"
	req.Header.Set("Date", now.Add(-10*time.Minute).Format(http.TimeFormat))
	digest := sha256.Sum256([]byte("tampered"))
	req.Header.Set("Digest", "SHA-256="+base64.StdEncoding.EncodeToString(digest[:]))
	req.Header.Set("Signature", `keyId="`+keyID+`",algorithm="rsa-sha256",headers="(request-target) host date digest",signature="AA=="`)
	_, err = VerifyRequest(context.Background(), req, body, fixedResolver{key: PublicKey{ID: keyID, Owner: owner, PEM: publicPEM}}, now)
	if err == nil {
		t.Fatal("expired signature with mismatched digest was accepted")
	}
}

func TestParseSignatureHeaderRejectsDuplicateAndMissingFields(t *testing.T) {
	for _, value := range []string{
		`keyId="https://remote.example/a#key",keyId="https://remote.example/b#key",algorithm="rsa-sha256",headers="date",signature="AA=="`,
		`keyId="https://remote.example/a#key",algorithm="rsa-sha256",headers="date"`,
		"not-a-signature",
	} {
		if _, err := parseSignatureHeader(value); err == nil {
			t.Fatalf("accepted malformed signature header %q", value)
		}
	}
}

func TestSafeRemoteDialRejectsPrivateAndSpecialPurposeAddresses(t *testing.T) {
	for _, address := range []string{
		"127.0.0.1:443", "10.0.0.1:443", "192.168.1.10:443", "169.254.1.1:443",
		"0.1.2.3:443", "100.64.0.1:443", "192.0.2.1:443", "192.88.99.1:443",
		"198.51.100.1:443", "203.0.113.1:443", "240.0.0.1:443",
		"[::1]:443", "[fc00::1]:443", "[fe80::1]:443", "[2001:db8::1]:443",
		"[64:ff9b::7f00:1]:443", "127.0.0.1:" + strconv.Itoa(80),
	} {
		if _, err := safePublicDialContext(context.Background(), "tcp", address); err == nil {
			t.Fatalf("accepted unsafe remote address %s", address)
		}
	}
}
