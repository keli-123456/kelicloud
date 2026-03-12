package linode

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

type Disk struct {
	ID         int    `json:"id"`
	Label      string `json:"label"`
	Status     string `json:"status"`
	Size       int    `json:"size"`
	Filesystem string `json:"filesystem"`
	Created    string `json:"created"`
	Updated    string `json:"updated"`
}

type Config struct {
	ID          int    `json:"id"`
	Label       string `json:"label"`
	Kernel      string `json:"kernel"`
	RootDevice  string `json:"root_device"`
	RunLevel    string `json:"run_level"`
	MemoryLimit int    `json:"memory_limit"`
	Comments    string `json:"comments"`
}

type Backup struct {
	ID       int    `json:"id"`
	Label    string `json:"label"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	Created  string `json:"created"`
	Updated  string `json:"updated"`
	Finished string `json:"finished"`
}

type BackupSchedule struct {
	Day    string `json:"day"`
	Window string `json:"window"`
}

type Backups struct {
	Enabled        bool           `json:"enabled"`
	Available      bool           `json:"available"`
	Schedule       BackupSchedule `json:"schedule"`
	LastSuccessful string         `json:"last_successful"`
	Snapshot       *Backup        `json:"snapshot"`
	Automatic      []Backup       `json:"automatic"`
}

type ResizeInstanceRequest struct {
	Type string `json:"type" binding:"required"`
}

type RebuildInstanceRequest struct {
	Image          string   `json:"image" binding:"required"`
	RootPass       string   `json:"root_pass" binding:"required"`
	AuthorizedKeys []string `json:"authorized_keys,omitempty"`
	Booted         bool     `json:"booted"`
	Metadata       *struct {
		UserData string `json:"user_data,omitempty"`
	} `json:"metadata,omitempty"`
}

func (c *Client) GetInstance(ctx context.Context, instanceID int) (*Instance, error) {
	return getObject[Instance](ctx, c, fmt.Sprintf("/v4/linode/instances/%d", instanceID), nil)
}

func (c *Client) ListInstanceDisks(ctx context.Context, instanceID int) ([]Disk, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[Disk](ctx, c, fmt.Sprintf("/v4/linode/instances/%d/disks", instanceID), query)
}

func (c *Client) ListInstanceConfigs(ctx context.Context, instanceID int) ([]Config, error) {
	query := url.Values{"page_size": {"500"}}
	return getPaginated[Config](ctx, c, fmt.Sprintf("/v4/linode/instances/%d/configs", instanceID), query)
}

func (c *Client) GetInstanceBackups(ctx context.Context, instanceID int) (*Backups, error) {
	return getObject[Backups](ctx, c, fmt.Sprintf("/v4/linode/instances/%d/backups", instanceID), nil)
}

func (c *Client) ResizeInstance(ctx context.Context, instanceID int, request ResizeInstanceRequest) error {
	return c.doEmpty(ctx, http.MethodPost, fmt.Sprintf("/v4/linode/instances/%d/resize", instanceID), nil, request)
}

func (c *Client) CreateInstanceSnapshot(ctx context.Context, instanceID int) (*Backup, error) {
	return postObject[Backup](ctx, c, fmt.Sprintf("/v4/linode/instances/%d/backups", instanceID), map[string]any{})
}

func (c *Client) ResetInstanceRootPassword(ctx context.Context, instanceID int, rootPass string) error {
	return c.doEmpty(ctx, http.MethodPost, fmt.Sprintf("/v4/linode/instances/%d/password", instanceID), nil, map[string]any{
		"root_pass": rootPass,
	})
}

func (c *Client) RebuildInstance(ctx context.Context, instanceID int, request RebuildInstanceRequest) (*Instance, error) {
	return postObject[Instance](ctx, c, fmt.Sprintf("/v4/linode/instances/%d/rebuild", instanceID), request)
}
