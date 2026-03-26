package telegram

type Addition struct {
	BotToken        string `json:"bot_token" required:"true"`
	ChatID          string `json:"chat_id" required:"false" help:"Optional default chat ID. User-scoped notification bindings can override this field."`
	MessageThreadID string `json:"message_thread_id" help:"Optional. Unique identifier of a message thread to which the message belongs; for supergroups only"`
	Endpoint        string `json:"endpoint" required:"true" default:"https://api.telegram.org/bot" help:"Telegram API endpoint, default is https://api.telegram.org/bot"`
}
