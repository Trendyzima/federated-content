package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ActorProfile struct {
	UserID string `json:"user_id"`
	Handle string `json:"handle"`
	DisplayName string `json:"display_name"`
	Summary string `json:"summary"`
	FederationEnabled bool `json:"federation_enabled"`
}

type Client struct {
	baseURL string
	token string
	http *http.Client
}

func New(baseURL, token string, client *http.Client) (*Client,error) {
	baseURL=strings.TrimRight(strings.TrimSpace(baseURL),"/")
	token=strings.TrimSpace(token)
	u,err:=url.Parse(baseURL)
	if err!=nil||u.Host==""||u.User!=nil||u.RawQuery!=""||u.Fragment!=""||(u.Scheme!="https"&&!(u.Scheme=="http"&&(u.Hostname()=="api"||u.Hostname()=="localhost"||u.Hostname()=="127.0.0.1"))) {
		return nil,errors.New("Backend URL must be HTTPS (HTTP only for private api or localhost)")
	}
	if len(token)<32||strings.ContainsAny(token,"\r\n") { return nil,errors.New("Backend federation token must contain at least 32 characters") }
	if client==nil { client=&http.Client{Timeout:5*time.Second} }
	copyClient:=*client
	copyClient.CheckRedirect=func(*http.Request,[]*http.Request)error{return http.ErrUseLastResponse}
	return &Client{baseURL:baseURL,token:token,http:&copyClient},nil
}

func (c *Client) ResolveActor(ctx context.Context, handle string) (ActorProfile,error) {
	handle=strings.ToLower(strings.TrimSpace(handle))
	if handle==""||len(handle)>30 { return ActorProfile{},errors.New("invalid local actor handle") }
	req,err:=http.NewRequestWithContext(ctx,http.MethodGet,c.baseURL+"/internal/v1/federation/actors/"+url.PathEscape(handle),nil)
	if err!=nil{return ActorProfile{},err}
	req.Header.Set("Authorization","Bearer "+c.token)
	req.Header.Set("Accept","application/json")
	resp,err:=c.http.Do(req)
	if err!=nil{return ActorProfile{},fmt.Errorf("Backend actor lookup failed: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode==http.StatusNotFound{return ActorProfile{},ErrActorNotFound}
	if resp.StatusCode<200||resp.StatusCode>=300{return ActorProfile{},fmt.Errorf("Backend actor lookup returned HTTP %d",resp.StatusCode)}
	var envelope struct {
		OK bool `json:"ok"`
		Data ActorProfile `json:"data"`
		Error *struct { Message string `json:"message"` } `json:"error"`
	}
	if err:=json.NewDecoder(http.MaxBytesReader(nil,resp.Body,64<<10)).Decode(&envelope);err!=nil{return ActorProfile{},errors.New("Backend actor lookup returned invalid JSON")}
	if !envelope.OK||!envelope.Data.FederationEnabled||envelope.Data.UserID==""||envelope.Data.Handle!=handle {
		return ActorProfile{},errors.New("Backend actor lookup returned an invalid or ineligible profile")
	}
	return envelope.Data,nil
}

func (c *Client) Ready(ctx context.Context) error {
	req,err:=http.NewRequestWithContext(ctx,http.MethodGet,c.baseURL+"/readyz",nil)
	if err!=nil{return err}
	resp,err:=c.http.Do(req)
	if err!=nil{return fmt.Errorf("Backend readiness check failed: %w",err)}
	defer resp.Body.Close()
	if resp.StatusCode!=http.StatusOK{return fmt.Errorf("Backend is not ready (HTTP %d)",resp.StatusCode)}
	return nil
}

var ErrActorNotFound=errors.New("local actor not found")
