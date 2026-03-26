package client

import (
	"errors"
	"net"
	"strings"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/clients"
	"github.com/komari-monitor/komari/utils/geoip"

	"github.com/gin-gonic/gin"
)

func getClientIPType(ip net.IP) int {
	// 0:ipv4 1:ipv6 -1:错误的输入
	if ip == nil {
		return -1
	}
	if ip.To4() == nil {
		return 1
	} else {
		return 0
	}
}

func populateBasicInfoFallbackIP(cbi map[string]interface{}, clientIP string) {
	if (func() bool {
		if v4, ok := cbi["ipv4"].(string); !ok || v4 == "" {
			if v6, ok := cbi["ipv6"].(string); !ok || v6 == "" {
				return true
			}
		}
		return false
	})() {
		ip := net.ParseIP(clientIP)
		ipType := getClientIPType(ip)

		switch ipType {
		case 0:
			cbi["ipv4"] = ip.String()
		case 1:
			cbi["ipv6"] = ip.String()
		default:
			break
		}
	}
}

func normalizeRegionCode(isoCode string) string {
	isoCode = strings.ToUpper(strings.TrimSpace(isoCode))
	if len(isoCode) != 2 {
		return ""
	}
	for _, r := range isoCode {
		if r < 'A' || r > 'Z' {
			return ""
		}
	}
	return isoCode
}

func setRegionFromGeoInfo(cbi map[string]interface{}, info *geoip.GeoInfo) string {
	if info == nil {
		return ""
	}

	regionCode := normalizeRegionCode(info.ISOCode)
	if regionCode == "" {
		return ""
	}

	if emoji := geoip.GetRegionUnicodeEmoji(regionCode); emoji != "" {
		cbi["region"] = emoji
	} else {
		cbi["region"] = regionCode
	}
	return regionCode
}

func populateBasicInfoRegion(cbi map[string]interface{}) string {
	if ipv4, ok := cbi["ipv4"].(string); ok && ipv4 != "" {
		ip4 := net.ParseIP(ipv4)
		ip4Record, _ := geoip.GetGeoInfo(ip4)
		if regionCode := setRegionFromGeoInfo(cbi, ip4Record); regionCode != "" {
			return regionCode
		}
	} else if ipv6, ok := cbi["ipv6"].(string); ok && ipv6 != "" {
		ip6 := net.ParseIP(ipv6)
		ip6Record, _ := geoip.GetGeoInfo(ip6)
		if regionCode := setRegionFromGeoInfo(cbi, ip6Record); regionCode != "" {
			return regionCode
		}
	}

	return ""
}

func shouldFallbackRegionToCode(err error) bool {
	var mysqlErr *mysqlDriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	if mysqlErr.Number != 1366 {
		return false
	}

	message := strings.ToLower(mysqlErr.Message)
	return strings.Contains(message, "column 'region'") || strings.Contains(message, "column `region`")
}

func UploadBasicInfo(c *gin.Context) {
	var cbi = map[string]interface{}{}
	if err := c.ShouldBindJSON(&cbi); err != nil {
		c.JSON(400, gin.H{"status": "error", "error": "Invalid or missing data"})
		return
	}

	uuidValue, ok := c.Get("client_uuid")
	uuid, _ := uuidValue.(string)
	if !ok || uuid == "" {
		c.JSON(400, gin.H{"status": "error", "error": "Invalid token"})
		return
	}

	cbi["uuid"] = uuid

	populateBasicInfoFallbackIP(cbi, c.ClientIP())

	regionCode := ""
	if cfg, err := config.GetAs[bool](config.GeoIpEnabledKey); err == nil && cfg {
		regionCode = populateBasicInfoRegion(cbi)
	}

	if err := clients.SaveClientInfo(cbi); err != nil {
		if regionCode != "" && shouldFallbackRegionToCode(err) {
			cbi["region"] = regionCode
			if retryErr := clients.SaveClientInfo(cbi); retryErr == nil {
				c.JSON(200, gin.H{"status": "success"})
				return
			} else {
				err = retryErr
			}
		}
		c.JSON(500, gin.H{"status": "error", "error": err})
		return
	}

	c.JSON(200, gin.H{"status": "success"})
}
