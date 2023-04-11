package adapter

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/huantt/kafka-dump/model"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"net/http"
	"time"
)

type AnalyticsRest struct {
	httpClient *resty.Client
}

func NewAnalyticsRest(APIEndpoint string) *AnalyticsRest {
	httpClient := resty.New()
	httpClient.
		SetRetryCount(12).
		SetRetryWaitTime(5 * time.Second).
		SetBaseURL(APIEndpoint).AddRetryCondition(func(response *resty.Response, err error) bool {
		if err != nil {
			return true
		}
		if response.StatusCode() == http.StatusInternalServerError ||
			response.StatusCode() == http.StatusBadGateway ||
			response.StatusCode() == http.StatusGatewayTimeout ||
			response.StatusCode() == http.StatusServiceUnavailable {
			log.Warnf("Response status code is %d - Request: %s - Body: %s - Retrying...", response.StatusCode(), response.Request.URL, response.Body())
			return true
		}

		return false
	})
	return &AnalyticsRest{httpClient}
}

func (i *AnalyticsRest) BulkInsert(ctx context.Context, records []model.NftBalance) error {
	if len(records) == 0 {
		return nil
	}
	resp, err := i.httpClient.R().SetBody(records).Post("/internal/nft-balance/insert-many")
	if err != nil {
		return errors.Wrapf(err, "[BulkInsert] %s", resp.Request.URL)
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("[BulkInsert] Status: %d - Request: %s", resp.StatusCode(), resp.Request.URL))
	}
	return nil
}
func (i *AnalyticsRest) DeleteOldVersionRecords(ctx context.Context, contractAddress string, latestVersion time.Time) error {
	resp, err := i.httpClient.R().
		SetQueryParam("latest_state", latestVersion.Format(time.RFC3339)).
		SetPathParam("contract", contractAddress).
		Delete("/internal/nft-balance/delete-old-version/{contract}")
	if err != nil {
		return errors.Wrapf(err, "[DeleteOldVersionRecords] %s", resp.Request.URL)
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("[DeleteOldVersionRecords] Status: %d - Request: %s", resp.StatusCode(), resp.Request.URL))
	}
	return nil
}
