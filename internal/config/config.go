package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	KafkaBrokers  string `envconfig:"KAFKA_BROKERS"  default:"localhost:9092"`
	KafkaTopic    string `envconfig:"KAFKA_TOPIC"    default:"activity-events"`
	KafkaGroupID  string `envconfig:"KAFKA_GROUP_ID" default:"activity-feed-worker"`
	ClickHouseDSN string `envconfig:"CLICKHOUSE_DSN" default:"clickhouse://localhost:9000/default"`
	HTTPAddr      string `envconfig:"HTTP_ADDR"      default:":8080"`
}

func Load() (Config, error) {
	var c Config
	return c, envconfig.Process("", &c)
}
