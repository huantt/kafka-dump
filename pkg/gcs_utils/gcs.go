package gcs_utils

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/huantt/kafka-dump/pkg/log"
	"google.golang.org/api/option"
)

var client *storage.Client

func Singleton(credentialsFile string) (*storage.Client, error) {
	if client != nil {
		return client, nil
	}
	newClient, err := storage.NewClient(context.Background(), option.WithCredentialsFile(credentialsFile))
	if err != nil {
		log.Fatalf("Failed to create GCS client: %v", err)
		return nil, err
	}
	return newClient, err
}
