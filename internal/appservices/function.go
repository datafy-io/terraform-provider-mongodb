package appservices

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Function is an App Services Function. Fields the provider does not manage (can_evaluate,
// run_as_user_id, run_as_user_id_script_source) are not sent, so an update clears them.
type Function struct {
	FunctionId  string `json:"_id,omitempty"`
	Name        string `json:"name"`
	Source      string `json:"source"`
	Private     bool   `json:"private"`
	RunAsSystem bool   `json:"run_as_system"`
}

type FunctionSummary struct {
	FunctionId string `json:"_id"`
	Name       string `json:"name"`
}

type CreateFunctionRequest struct {
	ProjectId string
	AppId     string
	Function  Function
}

type CreateFunctionResponse struct {
	Function Function
}

type GetFunctionRequest struct {
	ProjectId  string
	AppId      string
	FunctionId string
}

type GetFunctionResponse struct {
	Function Function
}

type ListFunctionsRequest struct {
	ProjectId string
	AppId     string
}

type ListFunctionsResponse struct {
	Functions []FunctionSummary
}

type UpdateFunctionRequest struct {
	ProjectId string
	AppId     string
	Function  Function
}

type UpdateFunctionResponse struct {
}

type DeleteFunctionRequest struct {
	ProjectId  string
	AppId      string
	FunctionId string
}

type DeleteFunctionResponse struct {
}

func functionsPath(projectId, appId string) string {
	return fmt.Sprintf("/groups/%s/apps/%s/functions", url.PathEscape(projectId), url.PathEscape(appId))
}

func (c *Client) CreateFunction(ctx context.Context, req *CreateFunctionRequest) (*CreateFunctionResponse, error) {
	fn := req.Function
	fn.FunctionId = ""

	resp, err := c.callAPI(ctx, http.MethodPost, functionsPath(req.ProjectId, req.AppId), fn)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, toError(resp)
	}

	var created FunctionSummary
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, err
	}

	fn.FunctionId = created.FunctionId
	return &CreateFunctionResponse{
		Function: fn,
	}, nil
}

func (c *Client) GetFunction(ctx context.Context, req *GetFunctionRequest) (*GetFunctionResponse, error) {
	resp, err := c.callAPI(ctx, http.MethodGet, functionsPath(req.ProjectId, req.AppId)+"/"+url.PathEscape(req.FunctionId), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, toError(resp)
	}

	var fn Function
	if err := json.NewDecoder(resp.Body).Decode(&fn); err != nil {
		return nil, err
	}

	return &GetFunctionResponse{
		Function: fn,
	}, nil
}

func (c *Client) ListFunctions(ctx context.Context, req *ListFunctionsRequest) (*ListFunctionsResponse, error) {
	resp, err := c.callAPI(ctx, http.MethodGet, functionsPath(req.ProjectId, req.AppId), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, toError(resp)
	}

	var functions []FunctionSummary
	if err := json.NewDecoder(resp.Body).Decode(&functions); err != nil {
		return nil, err
	}

	return &ListFunctionsResponse{
		Functions: functions,
	}, nil
}

// UpdateFunction replaces the whole function.
func (c *Client) UpdateFunction(ctx context.Context, req *UpdateFunctionRequest) (*UpdateFunctionResponse, error) {
	fn := req.Function
	functionId := fn.FunctionId
	fn.FunctionId = ""

	resp, err := c.callAPI(ctx, http.MethodPut, functionsPath(req.ProjectId, req.AppId)+"/"+url.PathEscape(functionId), fn)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return nil, toError(resp)
	}

	return &UpdateFunctionResponse{}, nil
}

func (c *Client) DeleteFunction(ctx context.Context, req *DeleteFunctionRequest) (*DeleteFunctionResponse, error) {
	resp, err := c.callAPI(ctx, http.MethodDelete, functionsPath(req.ProjectId, req.AppId)+"/"+url.PathEscape(req.FunctionId), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return nil, toError(resp)
	}

	return &DeleteFunctionResponse{}, nil
}
