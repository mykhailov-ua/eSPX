package costsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// MetaOAuthRefresher refreshes Facebook/Meta Marketing API tokens.
type MetaOAuthRefresher struct {
	AppID     string
	AppSecret string
	Client    *http.Client
}

func (r *MetaOAuthRefresher) Refresh(ctx context.Context, cred Credential) (string, time.Time, error) {
	if cred.RefreshToken == "" {
		return "", time.Time{}, fmt.Errorf("meta oauth: missing refresh token")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	q := url.Values{}
	q.Set("grant_type", "fb_exchange_token")
	q.Set("client_id", r.AppID)
	q.Set("client_secret", r.AppSecret)
	q.Set("fb_exchange_token", cred.RefreshToken)

	endpoint := "https://graph.facebook.com/v19.0/oauth/access_token?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("meta oauth refresh: status %d: %s", resp.StatusCode, string(body))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", time.Time{}, err
	}
	expires := time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second)
	return parsed.AccessToken, expires, nil
}

// GoogleOAuthRefresher refreshes Google Ads offline tokens.
type GoogleOAuthRefresher struct {
	ClientID     string
	ClientSecret string
	Client       *http.Client
}

func (r *GoogleOAuthRefresher) Refresh(ctx context.Context, cred Credential) (string, time.Time, error) {
	if cred.RefreshToken == "" {
		return "", time.Time{}, fmt.Errorf("google oauth: missing refresh token")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", cred.RefreshToken)
	form.Set("client_id", r.ClientID)
	form.Set("client_secret", r.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("google oauth refresh: status %d: %s", resp.StatusCode, string(body))
	}

	var parsed struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", time.Time{}, err
	}
	expires := time.Now().Add(time.Duration(parsed.ExpiresIn) * time.Second)
	return parsed.AccessToken, expires, nil
}
