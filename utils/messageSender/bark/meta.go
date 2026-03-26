package bark

import (
	"github.com/komari-monitor/komari/utils/messageSender/factory"
)

type Addition struct {
	ServerURL string `json:"server_url" required:"true" default:"https://api.day.app" help:"Bark server URL, e.g., https://api.day.app or your self-hosted server address"`
	DeviceKey string `json:"device_key" required:"false" help:"Optional default Bark device key. User-scoped notification bindings can override this field."`
	Icon      string `json:"icon" help:"Push notification icon, supports URL or system icon name"`
	Level     string `json:"level" type:"option" default:"timeSensitive" options:"active,timeSensitive,passive,critical" help:"Push notification level: active, timeSensitive (default), passive, critical"`
}

func init() {
	factory.RegisterMessageSender(func() factory.IMessageSender {
		return &BarkSender{}
	})
}
