package atproto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const publicAPIBaseURL = "https://public.api.bsky.app"

var (
	ErrInvalidHandle = errors.New("invalid AT Protocol handle")
	ErrNotFound = errors.New("AT Protocol actor not found")
	ErrUnavailable = errors.New("AT Protocol public API unavailable")
	handlePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)
)

type Client struct {
	baseURL string
	http *http.Client
}

type Profile struct {
	DID string `json:"did"`
	Handle string `json:"handle"`
	DisplayName string `json:"displayName,omitempty"`
	Description string `json:"description,omitempty"`
	Avatar string `json:"avatar,omitempty"`
}

type Post struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
	Text string `json:"text"`
	CreatedAt string `json:"createdAt,omitempty"`
	IndexedAt string `json:"indexedAt,omitempty"`
	Author Profile `json:"author"`
	ReplyCount int64 `json:"replyCount"`
	RepostCount int64 `json:"repostCount"`
	LikeCount int64 `json:"likeCount"`
	QuoteCount int64 `json:"quoteCount"`
	Labels []string `json:"labels,omitempty"`
}

type FeedPage struct {
	Profile Profile `json:"profile"`
	Posts []Post `json:"posts"`
	Cursor string `json:"cursor,omitempty"`
	Source string `json:"source"`
}

func NewClient() *Client {
	client, _ := NewClientWithBaseURL(publicAPIBaseURL, &http.Client{
		Timeout: 4*time.Second,
		CheckRedirect: func(*http.Request,[]*http.Request) error { return http.ErrUseLastResponse },
	})
	return client
}

// NewClientWithBaseURL permits deterministic transport tests. Production uses
// NewClient, which is pinned to the official public Bluesky API origin.
func NewClientWithBaseURL(base string, httpClient *http.Client) (*Client,error) {
	parsed,err:=url.Parse(strings.TrimRight(strings.TrimSpace(base),"/"))
	if err!=nil || parsed.Host=="" || parsed.User!=nil || parsed.RawQuery!="" || parsed.Fragment!="" || parsed.Path!="" {
		return nil,errors.New("AT Protocol API base must be an absolute origin")
	}
	if parsed.Scheme!="https" {
		ip:=net.ParseIP(parsed.Hostname())
		if parsed.Scheme!="http" || (parsed.Hostname()!="localhost" && (ip==nil || !ip.IsLoopback())) {
			return nil,errors.New("AT Protocol API must use HTTPS except for loopback tests")
		}
	}
	if httpClient==nil {
		httpClient=&http.Client{Timeout:4*time.Second,CheckRedirect:func(*http.Request,[]*http.Request)error{return http.ErrUseLastResponse}}
	}
	return &Client{baseURL:strings.TrimRight(base,"/"),http:httpClient},nil
}

func ValidHandle(handle string) bool {
	handle=strings.ToLower(strings.TrimSpace(handle))
	return len(handle)<=253 && handlePattern.MatchString(handle)
}

func validDID(did string) bool {
	did=strings.TrimSpace(did)
	if len(did)<12 || len(did)>512 || strings.ContainsAny(did," \t\r\n") { return false }
	return strings.HasPrefix(did,"did:plc:") || strings.HasPrefix(did,"did:web:")
}

func (c *Client) getJSON(ctx context.Context,path string,query url.Values,out any) error {
	if c==nil || c.http==nil { return ErrUnavailable }
	endpoint:=c.baseURL+path
	if len(query)>0 { endpoint+="?"+query.Encode() }
	req,err:=http.NewRequestWithContext(ctx,http.MethodGet,endpoint,nil)
	if err!=nil { return fmt.Errorf("%w: request construction",ErrUnavailable) }
	req.Header.Set("Accept","application/json")
	req.Header.Set("User-Agent","Testagram-Federation/1.0")
	resp,err:=c.http.Do(req)
	if err!=nil { return fmt.Errorf("%w: %v",ErrUnavailable,err) }
	defer resp.Body.Close()
	if resp.StatusCode==http.StatusNotFound { return ErrNotFound }
	if resp.StatusCode<200 || resp.StatusCode>=300 {
		_,_=io.Copy(io.Discard,io.LimitReader(resp.Body,4096))
		return fmt.Errorf("%w: upstream HTTP %d",ErrUnavailable,resp.StatusCode)
	}
	decoder:=json.NewDecoder(io.LimitReader(resp.Body,2<<20))
	if err:=decoder.Decode(out);err!=nil { return fmt.Errorf("%w: invalid upstream JSON",ErrUnavailable) }
	return nil
}

func (c *Client) ResolveProfile(ctx context.Context,handle string) (Profile,error) {
	handle=strings.ToLower(strings.TrimSpace(handle))
	if !ValidHandle(handle) { return Profile{},ErrInvalidHandle }
	var resolved struct { DID string `json:"did"` }
	if err:=c.getJSON(ctx,"/xrpc/com.atproto.identity.resolveHandle",url.Values{"handle":[]string{handle}},&resolved);err!=nil {
		return Profile{},err
	}
	if !validDID(resolved.DID) { return Profile{},fmt.Errorf("%w: resolver returned an invalid DID",ErrUnavailable) }
	var profile Profile
	if err:=c.getJSON(ctx,"/xrpc/app.bsky.actor.getProfile",url.Values{"actor":[]string{resolved.DID}},&profile);err!=nil {
		return Profile{},err
	}
	if !validDID(profile.DID) || profile.DID!=resolved.DID || !ValidHandle(strings.ToLower(profile.Handle)) {
		return Profile{},fmt.Errorf("%w: profile identity did not match the resolved actor",ErrUnavailable)
	}
	return profile,nil
}

func (c *Client) AuthorFeed(ctx context.Context,handle string,limit int,cursor string) (FeedPage,error) {
	profile,err:=c.ResolveProfile(ctx,handle)
	if err!=nil { return FeedPage{},err }
	if limit<1 || limit>25 { limit=20 }
	if len(cursor)>512 || strings.ContainsAny(cursor,"\r\n") { return FeedPage{},errors.New("invalid AT Protocol feed cursor") }
	query:=url.Values{"actor":[]string{profile.DID},"limit":[]string{fmt.Sprint(limit)},"filter":[]string{"posts_with_replies"}}
	if cursor!="" { query.Set("cursor",cursor) }
	var raw struct {
		Cursor string `json:"cursor"`
		Feed []struct {
			Post struct {
				URI string `json:"uri"`
				CID string `json:"cid"`
				Author Profile `json:"author"`
				Record struct {
					Text string `json:"text"`
					CreatedAt string `json:"createdAt"`
				} `json:"record"`
				IndexedAt string `json:"indexedAt"`
				ReplyCount int64 `json:"replyCount"`
				RepostCount int64 `json:"repostCount"`
				LikeCount int64 `json:"likeCount"`
				QuoteCount int64 `json:"quoteCount"`
				Labels []struct { Val string `json:"val"` } `json:"labels"`
			} `json:"post"`
		} `json:"feed"`
	}
	if err:=c.getJSON(ctx,"/xrpc/app.bsky.feed.getAuthorFeed",query,&raw);err!=nil { return FeedPage{},err }
	posts:=make([]Post,0,len(raw.Feed))
	for _,entry:=range raw.Feed {
		p:=entry.Post
		if !strings.HasPrefix(p.URI,"at://") || p.CID=="" || !validDID(p.Author.DID) { continue }
		labels:=make([]string,0,len(p.Labels))
		for _,label:=range p.Labels { if label.Val!="" && len(label.Val)<=80 { labels=append(labels,label.Val) } }
		posts=append(posts,Post{URI:p.URI,CID:p.CID,Text:p.Record.Text,CreatedAt:p.Record.CreatedAt,
			IndexedAt:p.IndexedAt,Author:p.Author,ReplyCount:p.ReplyCount,RepostCount:p.RepostCount,
			LikeCount:p.LikeCount,QuoteCount:p.QuoteCount,Labels:labels})
	}
	return FeedPage{Profile:profile,Posts:posts,Cursor:raw.Cursor,Source:"bluesky-atproto"},nil
}
