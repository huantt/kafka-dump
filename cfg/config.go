package config

import (
	"bytes"
	"encoding/json"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/spf13/viper"
	"strings"
)

type Config struct {
	Log       log.Config `json:"log" mapstructure:"log"`
	OutPath   string     `json:"out_path" mapstructure:"out_path"`
	InputPath string     `json:"input_path" mapstructure:"input_path"`
	Topics    string     `json:"topics" mapstructure:"topics"`

	Kafka kafka_utils.Config `json:"kafka" mapstructure:"kafka"`
}

var cfg *Config

func Load() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}
	// You should set default config value here
	c := &Config{
		Log: log.Config{
			Level:  "debug",
			Format: "text",
		},
	}

	// --- hacking to load reflect structure config into env ----//
	viper.SetConfigType("json")
	configBuffer, err := json.Marshal(c)

	if err != nil {
		return nil, err
	}

	if err := viper.ReadConfig(bytes.NewBuffer(configBuffer)); err != nil {
		panic(err)
	}
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	//err = viper.WriteConfigAs(".init.env")
	//fmt.Println(err)

	// -- end of hacking --//
	viper.AutomaticEnv()
	err = viper.Unmarshal(c)
	if err != nil {
		return nil, err
	}
	cfg = c
	return c, nil
}
