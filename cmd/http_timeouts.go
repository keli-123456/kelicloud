package cmd

import (
	"net/http"
	"time"

	"github.com/komari-monitor/komari/utils"
)

func applyHTTPTimeouts(srv *http.Server) {
	srv.ReadHeaderTimeout = utils.DurationFromEnv("KOMARI_HTTP_READ_HEADER_TIMEOUT", 10*time.Second)
	srv.ReadTimeout = utils.DurationFromEnv("KOMARI_HTTP_READ_TIMEOUT", 0)
	srv.WriteTimeout = utils.DurationFromEnv("KOMARI_HTTP_WRITE_TIMEOUT", 0)
	srv.IdleTimeout = utils.DurationFromEnv("KOMARI_HTTP_IDLE_TIMEOUT", 120*time.Second)
}
