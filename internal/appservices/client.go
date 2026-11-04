package appservices

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/datafy-io/terraform-provider-mongodb/version"
)

const DefaultBaseURL = "https://services.cloud.mongodb.com/api/admin/v3.0"

// ErrNotFound is returned when the API answers 404.
var ErrNotFound = errors.New("not found")

// Client calls the App Services Admin API with an Atlas programmatic API key.
// It logs in on the first call and keeps the session: access tokens last 30 minutes,
// so a 401 refreshes the token (or logs in again) and retries once.
type Client struct {
	publicKey  string
	privateKey string
	baseURL    string

	httpClient *http.Client

	mu           sync.Mutex
	accessToken  string
	refreshToken string
}

func NewClient(publicKey, privateKey, baseURL string) *Client {
	return &Client{
		publicKey:  publicKey,
		privateKey: privateKey,
		baseURL:    baseURL,

		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) callAPI(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var jsonData []byte
	if body != nil {
		var err error
		if jsonData, err = json.Marshal(body); err != nil {
			return nil, err
		}
	}

	token, err := c.token(ctx, "")
	if err != nil {
		return nil, err
	}

	resp, err := c.do(ctx, method, path, jsonData, token)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if token, err = c.token(ctx, token); err != nil {
		return nil, err
	}
	return c.do(ctx, method, path, jsonData, token)
}

func (c *Client) do(ctx context.Context, method, path string, jsonData []byte, token string) (*http.Response, error) {
	var reqBody io.Reader
	if jsonData != nil {
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("User-Agent", fmt.Sprintf("terraform-provider-mongodb/%s (datafy.io)", version.ProviderVersion))

	return c.httpClient.Do(req)
}

// token returns the current access token. If rejected is the current token (the API answered 401),
// it gets a new one: from the refresh token if the session is still valid, otherwise by logging in again.
func (c *Client) token(ctx context.Context, rejected string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.accessToken != "" && c.accessToken != rejected {
		return c.accessToken, nil
	}

	if rejected != "" && c.refreshToken != "" {
		if err := c.refreshSession(ctx); err == nil {
			return c.accessToken, nil
		}
	}

	if err := c.login(ctx); err != nil {
		return "", err
	}
	return c.accessToken, nil
}

func (c *Client) login(ctx context.Context) error {
	jsonData, err := json.Marshal(map[string]string{
		"username": c.publicKey,
		"apiKey":   c.privateKey,
	})
	if err != nil {
		return err
	}

	resp, err := c.do(ctx, http.MethodPost, "/auth/providers/mongodb-cloud/login", jsonData, "")
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("login with the Atlas API key failed: %w", toError(resp))
	}

	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return err
	}

	c.accessToken = session.AccessToken
	c.refreshToken = session.RefreshToken
	return nil
}

func (c *Client) refreshSession(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodPost, "/auth/session", nil, c.refreshToken)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return toError(resp)
	}

	var session struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		return err
	}

	c.accessToken = session.AccessToken
	return nil
}

func toError(res *http.Response) error {
	body, _ := io.ReadAll(res.Body)

	var errMessage struct {
		Error     string `json:"error,omitempty"`
		ErrorCode string `json:"error_code,omitempty"`
	}
	_ = json.Unmarshal(body, &errMessage)

	message := errMessage.Error
	if message == "" {
		message = string(body)
	}
	if errMessage.ErrorCode != "" {
		message = fmt.Sprintf("%s (%s)", message, errMessage.ErrorCode)
	}

	if res.StatusCode == http.StatusNotFound {
		return fmt.Errorf("status code %d: %s: %w", res.StatusCode, message, ErrNotFound)
	}
	return fmt.Errorf("status code %d: %s", res.StatusCode, message)
}
