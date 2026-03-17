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
	}

	return gin.H{
		"sitename":                  cst.Sitename,
		"description":               cst.Description,
		"custom_head":               cst.CustomHead,
		"custom_body":               cst.CustomBody,
		"oauth_enable":              cst.OAuthEnabled,
		"oauth_provider":            cst.OAuthProvider,
		"disable_password_login":    cst.DisablePasswordLogin,
		"allow_cors":                cst.AllowCors,
		"record_enabled":            cst.RecordEnabled,
		"record_preserve_time":      cst.RecordPreserveTime,
		"ping_record_preserve_time": cst.PingRecordPreserveTime,
	}, nil
}
