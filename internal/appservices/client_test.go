package appservices

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSession answers login and refresh, and accepts only the newest access token it issued.
type fakeSession struct {
	logins, refreshes int
	accessToken       string
	refreshFails      bool
}

func (s *fakeSession) handle(mux *http.ServeMux) {
	mux.HandleFunc("POST /auth/providers/mongodb-cloud/login", func(w http.ResponseWriter, r *http.Request) {
		var creds map[string]string
		_ = json.NewDecoder(r.Body).Decode(&creds)
		if creds["username"] != "public" || creds["apiKey"] != "private" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid username/password","error_code":"InvalidPassword"}`))
			return
		}
		s.logins++
		s.accessToken = "access-" + string(rune('0'+s.logins))
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": s.accessToken, "refresh_token": "refresh"})
	})
	mux.HandleFunc("POST /auth/session", func(w http.ResponseWriter, r *http.Request) {
		if s.refreshFails || r.Header.Get("Authorization") != "Bearer refresh" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		s.refreshes++
		s.accessToken = "refreshed-" + string(rune('0'+s.refreshes))
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": s.accessToken})
	})
}

func (s *fakeSession) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "Bearer "+s.accessToken {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"token expired"}`))
		return false
	}
	return true
}

func TestLoginOnce(t *testing.T) {
	s := &fakeSession{}
	mux := http.NewServeMux()
	s.handle(mux)
	mux.HandleFunc("GET /groups/p1/apps", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			_, _ = w.Write([]byte(`[]`))
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewClient("public", "private", ts.URL)
	for i := 0; i < 3; i++ {
		_, err := c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})
		require.NoError(t, err)
	}

	assert.Equal(t, 1, s.logins)
}

func TestExpiredTokenIsRefreshed(t *testing.T) {
	s := &fakeSession{}
	mux := http.NewServeMux()
	s.handle(mux)
	mux.HandleFunc("GET /groups/p1/apps", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			_, _ = w.Write([]byte(`[]`))
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewClient("public", "private", ts.URL)
	_, err := c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})
	require.NoError(t, err)

	s.accessToken = "expired" // the server stops accepting the client's token
	_, err = c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})
	require.NoError(t, err)

	assert.Equal(t, 1, s.logins)
	assert.Equal(t, 1, s.refreshes)
}

func TestFailedRefreshLogsInAgain(t *testing.T) {
	s := &fakeSession{refreshFails: true}
	mux := http.NewServeMux()
	s.handle(mux)
	mux.HandleFunc("GET /groups/p1/apps", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			_, _ = w.Write([]byte(`[]`))
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewClient("public", "private", ts.URL)
	_, err := c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})
	require.NoError(t, err)

	s.accessToken = "expired"
	_, err = c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})
	require.NoError(t, err)

	assert.Equal(t, 2, s.logins)
}

func TestBadKeyFails(t *testing.T) {
	s := &fakeSession{}
	mux := http.NewServeMux()
	s.handle(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewClient("public", "wrong", ts.URL)
	_, err := c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1"})

	assert.ErrorContains(t, err, "login with the Atlas API key failed: status code 401: invalid username/password (InvalidPassword)")
}

func TestListAppsSendsProduct(t *testing.T) {
	s := &fakeSession{}
	mux := http.NewServeMux()
	s.handle(mux)
	mux.HandleFunc("GET /groups/p1/apps", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(w, r) {
			return
		}
		assert.Equal(t, "product=standard,atlas", r.URL.RawQuery)
		_, _ = w.Write([]byte(`[{"_id":"a1","client_app_id":"triggers-abcde","name":"Triggers","product":"atlas","group_id":"p1"}]`))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewClient("public", "private", ts.URL)
	out, err := c.ListApps(context.Background(), &ListAppsRequest{ProjectId: "p1", Product: "standard,atlas"})

	require.NoError(t, err)
	assert.Equal(t, []App{{AppId: "a1", ClientAppId: "triggers-abcde", Name: "Triggers", Product: "atlas"}}, out.Apps)
}

func TestFunctionLifecycle(t *testing.T) {
	s := &fakeSession{}
	mux := http.NewServeMux()
	s.handle(mux)
	var created, updated map[string]interface{}
	mux.HandleFunc("POST /groups/p1/apps/a1/functions", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(w, r) {
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&created)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"_id":"f1","name":"job"}`))
	})
	mux.HandleFunc("GET /groups/p1/apps/a1/functions", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			_, _ = w.Write([]byte(`[{"_id":"f1","name":"job","last_modified":1700000000}]`))
		}
	})
	mux.HandleFunc("GET /groups/p1/apps/a1/functions/f1", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			_, _ = w.Write([]byte(`{"_id":"f1","name":"job","source":"exports = function () {};","private":true,"run_as_system":true,"can_evaluate":{}}`))
		}
	})
	mux.HandleFunc("PUT /groups/p1/apps/a1/functions/f1", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(w, r) {
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&updated)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /groups/p1/apps/a1/functions/f1", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("GET /groups/p1/apps/a1/functions/gone", func(w http.ResponseWriter, r *http.Request) {
		if s.authorized(w, r) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"function not found: 'gone'","error_code":"FunctionNotFound"}`))
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()
	c := NewClient("public", "private", ts.URL)
	fn := Function{Name: "job", Source: "exports = function () {};", Private: true, RunAsSystem: true}

	cfr, err := c.CreateFunction(ctx, &CreateFunctionRequest{ProjectId: "p1", AppId: "a1", Function: fn})
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"name": "job", "source": "exports = function () {};", "private": true, "run_as_system": true}, created)
	assert.Equal(t, "f1", cfr.Function.FunctionId)

	gfr, err := c.GetFunction(ctx, &GetFunctionRequest{ProjectId: "p1", AppId: "a1", FunctionId: "f1"})
	require.NoError(t, err)
	assert.Equal(t, Function{FunctionId: "f1", Name: "job", Source: "exports = function () {};", Private: true, RunAsSystem: true}, gfr.Function)

	lfr, err := c.ListFunctions(ctx, &ListFunctionsRequest{ProjectId: "p1", AppId: "a1"})
	require.NoError(t, err)
	assert.Equal(t, []FunctionSummary{{FunctionId: "f1", Name: "job"}}, lfr.Functions)

	fn.FunctionId = "f1"
	fn.Source = "exports = async function () {};"
	_, err = c.UpdateFunction(ctx, &UpdateFunctionRequest{ProjectId: "p1", AppId: "a1", Function: fn})
	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"name": "job", "source": "exports = async function () {};", "private": true, "run_as_system": true}, updated)

	_, err = c.DeleteFunction(ctx, &DeleteFunctionRequest{ProjectId: "p1", AppId: "a1", FunctionId: "f1"})
	require.NoError(t, err)

	_, err = c.GetFunction(ctx, &GetFunctionRequest{ProjectId: "p1", AppId: "a1", FunctionId: "gone"})
	assert.True(t, errors.Is(err, ErrNotFound))
	assert.ErrorContains(t, err, "status code 404: function not found: 'gone' (FunctionNotFound)")
}
