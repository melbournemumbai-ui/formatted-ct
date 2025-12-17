// client.go
package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
)

const (
	maxErrMsgLen     = 256
	defaultTenant    = "anonymous"
	defaultUserAgent = "mimir-continuous-test"
)

// MimirClient is the interface implemented by a client used to interact with Mimir.
type MimirClient interface {
	// WriteSeries writes input series to Mimir. Returns the response status code and optionally
	// an error. The error is always returned if request was not successful (eg. received a 4xx or 5xx error).
	WriteSeries(ctx context.Context, series []prompb.TimeSeries) (statusCode int, err error)

	// QueryRange performs a range query.
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error)

	// Query performs an instant query.
	Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error)
}

type ClientConfig struct {
	TenantID          string
	BasicAuthUser     string
	BasicAuthPassword string
	BearerToken       string

	WriteBaseEndpoint flagext.URLValue
	WriteBatchSize    int
	WriteTimeout      time.Duration
	WriteProtocol     string

	ReadBaseEndpoint flagext.URLValue
	ReadTimeout      time.Duration

	RequestDebug bool
	UserAgent    string
}

func (cfg *ClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TenantID, "tests.tenant-id", defaultTenant, "The tenant ID to use to write and read metrics in tests.")
	f.StringVar(&cfg.BasicAuthUser, "tests.basic-auth-user", "", "The username to use for HTTP bearer authentication. (mutually exclusive with bearer-token flag)")
	f.StringVar(&cfg.BasicAuthPassword, "tests.basic-auth-password", "", "The password to use for HTTP bearer authentication. (mutually exclusive with bearer-token flag)")
	f.StringVar(&cfg.BearerToken, "tests.bearer-token", "", "The bearer token to use for HTTP bearer authentication. (mutually exclusive with basic-auth flags)")

	f.Var(&cfg.WriteBaseEndpoint, "tests.write-endpoint", "The base endpoint on the write path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example /api/v1/push for the remote write API endpoint, so the configured URL must not include it.")
	f.IntVar(&cfg.WriteBatchSize, "tests.write-batch-size", 1000, "The maximum number of series to write in a single request.")
	f.DurationVar(&cfg.WriteTimeout, "tests.write-timeout", 5*time.Second, "The timeout for a single write request.")
	f.StringVar(&cfg.WriteProtocol, "tests.write-protocol", "prometheus", "The protocol to use to write series data. Supported values are: prometheus, otlp-http")

	f.Var(&cfg.ReadBaseEndpoint, "tests.read-endpoint", "The base endpoint on the read path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example /api/v1/query_range for range query API, so the configured URL must not include it.")
	f.DurationVar(&cfg.ReadTimeout, "tests.read-timeout", 60*time.Second, "The timeout for a single read request.")
	f.BoolVar(&cfg.RequestDebug, "tests.send-chunks-debugging-header", false, "Request debugging on the server side via header.")
	f.StringVar(&cfg.UserAgent, "tests.client.user-agent", defaultUserAgent, "The value the Mimir client should send in the User-Agent header.")
}

type Client struct {
	WriteClient ClientWriter
	ReadClient  v1.API
	Cfg         ClientConfig
	Logger      log.Logger
}

type ClientWriter interface {
	SendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error)
}

func NewClient(cfg ClientConfig, logger log.Logger) (*Client, error) {
	// Simple roundtripper
	rt := http.DefaultTransport
	rt = &ClientRoundTripper{
		TenantID:          cfg.TenantID,
		BasicAuthUser:     cfg.BasicAuthUser,
		BasicAuthPassword: cfg.BasicAuthPassword,
		BearerToken:       cfg.BearerToken,
		Rt:                rt,
		RequestDebug:      cfg.RequestDebug,
		UserAgent:         cfg.UserAgent,
	}

	// Endpoint validation
	if cfg.WriteBaseEndpoint.URL == nil {
		return nil, errors.New("write endpoint not set")
	}
	if cfg.ReadBaseEndpoint.URL == nil {
		return nil, errors.New("read endpoint not set")
	}

	// Auth conflict check
	if (cfg.TenantID != defaultTenant && cfg.BasicAuthUser != "" && cfg.BasicAuthPassword != "" && cfg.BearerToken != "") ||
		(cfg.BasicAuthUser != "" && cfg.BasicAuthPassword != "" && cfg.BearerToken != "") {
		return nil, errors.New("invalid auth configuration")
	}

	apiCfg := api.Config{
		Address:      cfg.ReadBaseEndpoint.String(),
		RoundTripper: rt,
	}
	readClient, err := api.NewClient(apiCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create read client: %w", err)
	}

	var writeClient ClientWriter
	switch cfg.WriteProtocol {
	case "prometheus":
		writeClient = &PrometheusWriter{
			HttpClient:        &http.Client{Transport: rt},
			WriteBaseEndpoint: cfg.WriteBaseEndpoint,
			WriteBatchSize:    cfg.WriteBatchSize,
			WriteTimeout:      cfg.WriteTimeout,
		}
	case "otlp-http":
		writeClient = &OtlpHTTPWriter{
			HttpClient:        &http.Client{Transport: rt},
			WriteBaseEndpoint: cfg.WriteBaseEndpoint,
			WriteBatchSize:    cfg.WriteBatchSize,
			WriteTimeout:      cfg.WriteTimeout,
		}
	default:
		return nil, fmt.Errorf("unsupported write protocol: %s", cfg.WriteProtocol)
	}

	return &Client{
		WriteClient: writeClient,
		ReadClient:  v1.NewAPI(readClient),
		Cfg:         cfg,
		Logger:      logger,
	}, nil
}

// QueryRange implements MimirClient.
func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error) {
	ctx = contextWithRequestOptions(ctx, options...)
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.ReadTimeout)
	defer cancel()

	ctx = querierapi.ContextWithReadConsistencyLevel(ctx, querierapi.ReadConsistencyStrong)

	value, _, err := c.ReadClient.QueryRange(ctx, query, v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	if err != nil {
		return nil, err
	}

	if value.Type() != model.ValMatrix {
		return nil, fmt.Errorf("was expecting to get a Matrix, but got %s", value.Type().String())
	}

	matrix, ok := value.(model.Matrix)
	if !ok {
		return nil, fmt.Errorf("failed to cast type to Matrix, type was %T", value)
	}

	return matrix, nil
}

// Query implements MimirClient.
func (c *Client) Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error) {
	ctx = contextWithRequestOptions(ctx, options...)
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.ReadTimeout)
	defer cancel()

	ctx = querierapi.ContextWithReadConsistencyLevel(ctx, querierapi.ReadConsistencyStrong)

	value, _, err := c.ReadClient.Query(ctx, query, ts)
	if err != nil {
		return nil, err
	}

	if value.Type() != model.ValVector {
		return nil, fmt.Errorf("was expecting to get a Vector, but got %s", value.Type().String())
	}

	vector, ok := value.(model.Vector)
	if !ok {
		return nil, fmt.Errorf("failed to cast type to Vector, type was %T", value)
	}

	return vector, nil
}

// WriteSeries implements MimirClient.
func (c *Client) WriteSeries(ctx context.Context, series []prompb.TimeSeries) (int, error) {
	lastStatusCode := 0

	// Honor the batch size.
	for len(series) > 0 {
		end := min(len(series), c.Cfg.WriteBatchSize)
		batch := series[0:end]
		series = series[end:]

		var err error
		lastStatusCode, err = c.WriteClient.SendWriteRequest(ctx, &prompb.WriteRequest{Timeseries: batch})
		if err != nil {
			return lastStatusCode, err
		}
	}

	return lastStatusCode, nil
}

// RequestOption defines a functional-style request option.
type RequestOption func(options *RequestOptions)

// WithResultsCacheEnabled controls whether the query-frontend results cache should be enabled or disabled for the request.
// This function assumes query-frontend results cache is enabled by default.
func WithResultsCacheEnabled(enabled bool) RequestOption {
	return func(options *RequestOptions) {
		options.ResultsCacheDisabled = !enabled
	}
}

func WithAdditionalHeaders(headers map[string]string) RequestOption {
	return func(options *RequestOptions) {
		options.additionalHeaders = headers
	}
}

// contextWithRequestOptions returns a context.Context with the request options applied.
func contextWithRequestOptions(ctx context.Context, options ...RequestOption) context.Context {
	actual := &RequestOptions{}
	for _, option := range options {
		option(actual)
	}

	return context.WithValue(ctx, RequestOptionsKey, actual)
}

type RequestOptions struct {
	ResultsCacheDisabled bool
	additionalHeaders    map[string]string
}

type key int

var RequestOptionsKey key

type ClientRoundTripper struct {
	Rt                http.RoundTripper
	TenantID          string
	BasicAuthUser     string
	BasicAuthPassword string
	BearerToken       string
	RequestDebug      bool
	UserAgent         string
}

// RoundTrip implements the http.RoundTripper interface.
func (rt *ClientRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Add authentication headers if configured
	if rt.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+rt.BearerToken)
	} else if rt.BasicAuthUser != "" || rt.BasicAuthPassword != "" {
		req.SetBasicAuth(rt.BasicAuthUser, rt.BasicAuthPassword)
	}

	// Add tenant ID if configured
	if rt.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", rt.TenantID)
	}

	// Add User-Agent if configured
	if rt.UserAgent != "" {
		req.Header.Set("User-Agent", rt.UserAgent)
	}

	// Make the request using the underlying transport
	return rt.Rt.RoundTrip(req)
}
