package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/datafy-io/terraform-provider-mongodb/internal/appservices"
)

// fakeAdminAPI is an in-memory App Services Admin API: login, apps and functions.
type fakeAdminAPI struct {
	*httptest.Server

	mu        sync.Mutex
	apps      []appservices.App
	functions map[string]appservices.Function // by app ID + "/" + function ID
	nextId    int
	creates   int
}

const (
	testProjectId = "6538c565b444f95fd4efc903"
	testAppId     = "65f0a1b2c3d4e5f6a7b8c9d0"
)

func newFakeAdminAPI(t *testing.T) *fakeAdminAPI {
	f := &fakeAdminAPI{
		apps: []appservices.App{
			{AppId: testAppId, ClientAppId: "triggers-abcde", Name: "Triggers", Product: "atlas"},
			{AppId: "65f0a1b2c3d4e5f6a7b8c9d1", ClientAppId: "web-fghij", Name: "web", Product: "standard"},
		},
		functions: map[string]appservices.Function{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/providers/mongodb-cloud/login", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "token", "refresh_token": "refresh"})
	})
	mux.HandleFunc("GET /groups/{groupId}/apps", f.authorized(f.listApps))
	mux.HandleFunc("GET /groups/{groupId}/apps/{appId}/functions", f.authorized(f.listFunctions))
	mux.HandleFunc("POST /groups/{groupId}/apps/{appId}/functions", f.authorized(f.createFunction))
	mux.HandleFunc("GET /groups/{groupId}/apps/{appId}/functions/{functionId}", f.authorized(f.getFunction))
	mux.HandleFunc("PUT /groups/{groupId}/apps/{appId}/functions/{functionId}", f.authorized(f.updateFunction))
	mux.HandleFunc("DELETE /groups/{groupId}/apps/{appId}/functions/{functionId}", f.authorized(f.deleteFunction))

	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeAdminAPI) authorized(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.PathValue("groupId") != testProjectId {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		next(w, r)
	}
}

// listApps returns only `standard` apps unless product is given, as the real API does.
func (f *fakeAdminAPI) listApps(w http.ResponseWriter, r *http.Request) {
	products := []string{"standard"}
	if p := r.URL.Query().Get("product"); p != "" {
		products = strings.Split(p, ",")
	}
	apps := []appservices.App{}
	for _, a := range f.apps {
		if slices.Contains(products, a.Product) {
			apps = append(apps, a)
		}
	}
	_ = json.NewEncoder(w).Encode(apps)
}

func (f *fakeAdminAPI) listFunctions(w http.ResponseWriter, r *http.Request) {
	summaries := []appservices.FunctionSummary{}
	for key, fn := range f.functions {
		if strings.HasPrefix(key, r.PathValue("appId")+"/") {
			summaries = append(summaries, appservices.FunctionSummary{FunctionId: fn.FunctionId, Name: fn.Name})
		}
	}
	_ = json.NewEncoder(w).Encode(summaries)
}

func (f *fakeAdminAPI) createFunction(w http.ResponseWriter, r *http.Request) {
	var fn appservices.Function
	_ = json.NewDecoder(r.Body).Decode(&fn)
	for key, existing := range f.functions {
		if strings.HasPrefix(key, r.PathValue("appId")+"/") && existing.Name == fn.Name {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"function name already exists","error_code":"FunctionDuplicateName"}`))
			return
		}
	}
	f.creates++
	fn.FunctionId = f.addFunction(r.PathValue("appId"), fn)
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(appservices.FunctionSummary{FunctionId: fn.FunctionId, Name: fn.Name})
}

func (f *fakeAdminAPI) getFunction(w http.ResponseWriter, r *http.Request) {
	fn, ok := f.functions[r.PathValue("appId")+"/"+r.PathValue("functionId")]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"function not found","error_code":"FunctionNotFound"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(fn)
}

func (f *fakeAdminAPI) updateFunction(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("appId") + "/" + r.PathValue("functionId")
	if _, ok := f.functions[key]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	var fn appservices.Function
	_ = json.NewDecoder(r.Body).Decode(&fn)
	fn.FunctionId = r.PathValue("functionId")
	f.functions[key] = fn
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeAdminAPI) deleteFunction(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("appId") + "/" + r.PathValue("functionId")
	if _, ok := f.functions[key]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	delete(f.functions, key)
	w.WriteHeader(http.StatusNoContent)
}

// addFunction stores a function as if it had been made outside Terraform (e.g. in the Atlas UI), and returns its ID.
func (f *fakeAdminAPI) addFunction(appId string, fn appservices.Function) string {
	f.nextId++
	fn.FunctionId = fmt.Sprintf("66%022x", f.nextId)
	f.functions[appId+"/"+fn.FunctionId] = fn
	return fn.FunctionId
}

func (f *fakeAdminAPI) function(appId, functionId string) (appservices.Function, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn, ok := f.functions[appId+"/"+functionId]
	return fn, ok
}

func (f *fakeAdminAPI) providerConfig() string {
	return fmt.Sprintf(`
provider "mongodb" {
  atlas_public_key      = "public"
  atlas_private_key     = "private"
  app_services_base_url = %q
}
`, f.URL)
}
