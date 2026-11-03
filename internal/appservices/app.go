package appservices

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// AllProducts lists every app product type. Atlas Triggers made in the Atlas UI live in an app of type `atlas`,
// which the API leaves out unless asked for.
const AllProducts = "standard,atlas,data-api,device-sync"

type App struct {
	AppId       string `json:"_id"`
	ClientAppId string `json:"client_app_id"`
	Name        string `json:"name"`
	Product     string `json:"product"`
}

type ListAppsRequest struct {
	ProjectId string
	Product   string
}

type ListAppsResponse struct {
	Apps []App
}

func (c *Client) ListApps(ctx context.Context, req *ListAppsRequest) (*ListAppsResponse, error) {
	path := fmt.Sprintf("/groups/%s/apps", url.PathEscape(req.ProjectId))
	if req.Product != "" {
		// Comma-separated product types, sent unescaped as the API documents.
		path += "?product=" + req.Product
	}

	resp, err := c.callAPI(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, toError(resp)
	}

	var apps []App
	if err := json.NewDecoder(resp.Body).Decode(&apps); err != nil {
		return nil, err
	}

	return &ListAppsResponse{
		Apps: apps,
	}, nil
}
