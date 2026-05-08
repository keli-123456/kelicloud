package database

import (
	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/config"
)

func GetPublicInfo() (map[string]interface{}, error) {
	cstPtr, err := config.GetManyAs[config.Legacy]()
	if err != nil {
		return nil, err
	}
	cst := *cstPtr

	all, allErr := config.GetAll()
	hasKey := func(k string) bool {
		if allErr != nil {
			return false
		}
		_, ok := all[k]
		return ok
	}

	// Apply defaults only when a key is missing.
	if !hasKey("sitename") {
		cst.Sitename = "Komari"
	}
	if !hasKey("description") {
		cst.Description = "Komari Monitor, a simple server monitoring tool."
	}
	if !hasKey("site_subtitle") {
		cst.SiteSubtitle = "Komari Monitor"
	}
	if !hasKey("github_url") {
		cst.GithubURL = "https://github.com/keli-123456"
	}
	if !hasKey("o_auth_provider") {
		cst.OAuthProvider = "github"
	}
	if !hasKey("record_enabled") {
		cst.RecordEnabled = true
	}
	if !hasKey("record_preserve_time") {
		cst.RecordPreserveTime = 720
	}
	if !hasKey("ping_record_preserve_time") {
		cst.PingRecordPreserveTime = 24
	}

	// Fallback defaults if we couldn't enumerate keys.
	if allErr != nil {
		if cst.Sitename == "" {
			cst.Sitename = "Komari"
		}
		if cst.Description == "" {
			cst.Description = "Komari Monitor, a simple server monitoring tool."
		}
		if cst.SiteSubtitle == "" {
			cst.SiteSubtitle = "Komari Monitor"
		}
		if cst.GithubURL == "" {
			cst.GithubURL = "https://github.com/keli-123456"
		}
	}

	return gin.H{
		"sitename":                  cst.Sitename,
		"description":               cst.Description,
		"site_subtitle":             cst.SiteSubtitle,
		"github_url":                cst.GithubURL,
		"custom_head":               "",
		"custom_body":               "",
		"oauth_enable":              false,
		"oauth_provider":            "",
		"disable_password_login":    false,
		"allow_cors":                cst.AllowCors,
		"record_enabled":            cst.RecordEnabled,
		"record_preserve_time":      cst.RecordPreserveTime,
		"ping_record_preserve_time": cst.PingRecordPreserveTime,
	}, nil
}
