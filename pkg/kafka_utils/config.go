package kafka_utils

type Config struct {
	BootstrapServers          string `json:"bootstrap_servers" mapstructure:"bootstrap_servers"`
	ClientID                  string `json:"client_id" mapstructure:"client_id"`
	SecurityProtocol          string `json:"security_protocol" mapstructure:"security_protocol"`
	SASLMechanism             string `json:"sasl_mechanism" mapstructure:"sasl_mechanism"`
	SASLUsername              string `json:"sasl_username" mapstructure:"sasl_username"`
	SASLPassword              string `json:"sasl_password" mapstructure:"sasl_password"`
	ReadTimeoutSeconds        int16  `json:"read_timeout_seconds" mapstructure:"read_timeout_seconds"`
	GroupId                   string `json:"group_id" mapstructure:"group_id"`
	QueueBufferingMaxMessages int    `json:"queue_buffering_max_messages" mapstructure:"queue_buffering_max_messages"`
	QueuedMaxMessagesKbytes   int64  `json:"queued_max_messages_kbytes" mapstructure:"queued_max_messages_kbytes"`
	FetchMessageMaxBytes      int64  `json:"fetch_message_max_bytes" mapstructure:"fetch_message_max_bytes"`
	SSLCALocation             string `json:"ssl_ca_location" mapstructure:"ssl_ca_location"`
	SSLKeyLocation            string `json:"ssl_key_location" mapstructure:"ssl_key_location"`
	SSLCertLocation           string `json:"ssl_certificate_location" mapstructure:"ssl_certificate_location"`
	SSLKeyPassword            string `json:"ssl_key_password" mapstructure:"ssl_key_password"`
	EnableAutoOffsetStore     bool   `json:"enable_auto_offset_store" mapstructure:"enable_auto_offset_store"`
}
