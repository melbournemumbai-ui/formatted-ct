// SPDX-License-Identifier: AGPL-3.0-only

package ospreycontinuoustest

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"

	// "log"

	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/continuoustest"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	chunkinfologger "github.com/grafana/mimir/pkg/util/chunkinfologger"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prompb "github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen     = 256
	defaultTenant    = "anonymous"
	defaultUserAgent = "mimir-continuous-test"
)

// checkEnvVars prints warnings for commonly required env vars if not set.
func checkEnvVars() {
	requiredVars := map[string]string{
		"WRITE_BASE_ENDPOINT": os.Getenv("WRITE_BASE_ENDPOINT"),
		"READ_BASE_ENDPOINT":  os.Getenv("READ_BASE_ENDPOINT"),
		"CLIENT_CERT_FILE":    os.Getenv("CLIENT_CERT_FILE"),
		"CLIENT_KEY_FILE":     os.Getenv("CLIENT_KEY_FILE"),
		"CA_CERT_FILE":        os.Getenv("CA_CERT_FILE"),
	}

	for varName, value := range requiredVars {
		if value == "" {
			fmt.Printf("Warning: Environment variable %s is not set.\n", varName)
		}
	}
}

func (rt *clientRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	options, _ := req.Context().Value(continuoustest.RequestOptionsKey).(*continuoustest.RequestOptions)
	if options != nil && options.ResultsCacheDisabled {
		// Despite the name, the "no-store" directive also disables results cache lookup in Mimir.
		req.Header.Set("Cache-Control", "no-store")
	}

	if rt.Ct.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+rt.Ct.BearerToken)
		if rt.Ct.TenantID != defaultTenant {
			req.Header.Set("X-Scope-OrgID", rt.Ct.TenantID)
		}
	} else if rt.Ct.BasicAuthUser != "" && rt.Ct.BasicAuthPassword != "" {
		req.SetBasicAuth(rt.Ct.BasicAuthUser, rt.Ct.BasicAuthPassword)
		if rt.Ct.TenantID != defaultTenant {
			req.Header.Set("X-Scope-OrgID", rt.Ct.TenantID)
		}
	} else {
		req.Header.Set("X-Scope-OrgID", rt.Ct.TenantID)
	}

	if rt.SourceTenants != "" {
		req.Header.Set("X-Source-Tenants", rt.SourceTenants)
	}

	if rt.Ct.UserAgent != "" {
		req.Header.Set("User-Agent", rt.Ct.UserAgent)
	}

	if lvl, ok := querierapi.ReadConsistencyLevelFromContext(req.Context()); ok {
		req.Header.Add(querierapi.ReadConsistencyHeader, lvl)
	}

	if rt.Ct.RequestDebug {
		req.Header.Add(chunkinfologger.ChunkInfoLoggingHeader, "series_id")
	}

	return rt.Ct.Rt.RoundTrip(req)
}

// OspreyClientConfig extends the base ClientConfig with organization-specific TLS and other fields.
type OspreyClientConfig struct {
	Ct continuoustest.ClientConfig

	ClientCertFile string // Path to the client certificate
	ClientKeyFile  string // Path to the client key file
	CACertFile     string // Path to the CA certificate file

	IngestCertFile string
	IngestKeyFile  string
	QueryCertFile  string
	QueryKeyFile   string
	AlertsCertFile string
	AlertsKeyFile  string
	// WriteTransport       http.RoundTripper
	// ReadTransport        http.RoundTripper

	ReadPromBaseEndpoint flagext.URLValue
	SourceTenants        string
}

type OspreyClient struct {
	WriteClient clientWriter
	ReadClient  v1.API
	httpClient  *http.Client
	Cfg         OspreyClientConfig
	Logger      log.Logger
}

type clientWriter interface {
	SendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error)
}

// Alert is a simplified alert representation returned by ListActiveAlerts.
type Alert struct {
	Name   string
	State  string
	Labels map[string]string
}

func (cfg *OspreyClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Ct.TenantID, "tests.tenant-id", defaultTenant, "The tenant ID to use to write and read metrics in tests.")
	f.StringVar(&cfg.Ct.BasicAuthUser, "tests.basic-auth-user", "", "The username to use for HTTP bearer authentication. (mutually exclusive with bearer-token flag)")
	f.StringVar(&cfg.Ct.BasicAuthPassword, "tests.basic-auth-password", "", "The password to use for HTTP bearer authentication. (mutually exclusive with bearer-token flag)")
	f.StringVar(&cfg.Ct.BearerToken, "tests.bearer-token", "", "The bearer token to use for HTTP bearer authentication. (mutually exclusive with basic-auth flags)")

	writeEndpoint := os.Getenv("WRITE_BASE_ENDPOINT")
	f.Var(&cfg.Ct.WriteBaseEndpoint, "tests.write-endpoint", writeEndpoint)

	f.IntVar(&cfg.Ct.WriteBatchSize, "tests.write-batch-size", 1000, "The maximum number of series to write in a single request.")
	f.DurationVar(&cfg.Ct.WriteTimeout, "tests.write-timeout", 5*time.Second, "The timeout for a single write request.")
	f.StringVar(&cfg.Ct.WriteProtocol, "tests.write-protocol", "prometheus", "The protocol to use to write series data. Supported values are: prometheus, otlp-http")

	readEndpoint := os.Getenv("READ_BASE_ENDPOINT")
	f.Var(&cfg.Ct.ReadBaseEndpoint, "tests.read-endpoint", readEndpoint)
	f.DurationVar(&cfg.Ct.ReadTimeout, "tests.read-timeout", 60*time.Second, "The timeout for a single read request.")
	f.BoolVar(&cfg.Ct.RequestDebug, "tests.send-chunks-debugging-header", false, "Request debugging on the server side via header.")
	f.StringVar(&cfg.Ct.UserAgent, "tests.client.user-agent", defaultUserAgent, "The value the Mimir client should send in the User-Agent header.")

	certFile := os.Getenv("CLIENT_CERT_FILE")
	f.StringVar(&cfg.ClientCertFile, "tests.client-cert-file", certFile, "Path to the client certificate file.")
	keyFile := os.Getenv("CLIENT_KEY_FILE")
	f.StringVar(&cfg.ClientKeyFile, "tests.client-key-file", keyFile, "Path to the client key file.")
	caFile := os.Getenv("CA_CERT_FILE")
	f.StringVar(&cfg.CACertFile, "tests.ca-cert-file", caFile, "Path to the CA certificate file.")

	f.Var(&cfg.ReadPromBaseEndpoint, "tests.read-prom-endpoint", "optional base endpoint for PromQL APIs. If unset, defaults to --tests.read-endpoint.")
	f.StringVar(&cfg.SourceTenants, "tests.source-tenants-header", "", "Optional value for X-Source-Tenants header on requests.")

	f.StringVar(&cfg.IngestCertFile, "tests.ingest-cert-file", os.Getenv("INGEST_CERT_FILE"), "Client certificate for ingestion (remote write). If empty, falls back to query cert.")
	f.StringVar(&cfg.IngestKeyFile, "tests.ingest-key-file", os.Getenv("INGEST_KEY_FILE"), "Client key for ingestion.")
	f.StringVar(&cfg.QueryCertFile, "tests.query-cert-file", os.Getenv("QUERY_CERT_FILE"), "Client certificate for queries (required).")
	f.StringVar(&cfg.QueryKeyFile, "tests.query-key-file", os.Getenv("QUERY_KEY_FILE"), "Client key for queries (required).")
	f.StringVar(&cfg.AlertsCertFile, "tests.alerts-cert-file", os.Getenv("ALERTS_CERT_FILE"), "Client certificate for alerts/rules/AM (required).")
	f.StringVar(&cfg.AlertsKeyFile, "tests.alerts-key-file", os.Getenv("ALERTS_KEY_FILE"), "Client key for alerts/rules/AM (required).")

}

type clientRoundTripper struct {
	Ct                continuoustest.ClientRoundTripper
	WriteBaseEndpoint flagext.URLValue
	ReadBaseEndpoint  flagext.URLValue
	SourceTenants     string
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// makeTLS creates a TLS configuration using the provided certificate, key, and CA files.
// makeTLS creates a TLS configuration using the provided certificate, key, and CA files.
// func makeTLS(certFile, keyFile, caFile string, logger log.Logger) (*tls.Config, error) {
//     // Load client certificate
//     cert, err := tls.LoadX509KeyPair(certFile, keyFile)
//     if err != nil {
//         return nil, fmt.Errorf("error loading client certificate: %w", err)
//     }

//     // Create a new cert pool with system CAs
//     rootCAs, err := x509.SystemCertPool()
//     if err != nil {
//         return nil, fmt.Errorf("failed to load system CA pool: %w", err)
//     }
//     if rootCAs == nil {
//         rootCAs = x509.NewCertPool()
//     }

//     // Load and append custom CA certificates
//     caPEM, err := os.ReadFile(caFile)
//     if err != nil {
//         return nil, fmt.Errorf("error reading CA certificate: %w", err)
//     }

//     // Add all certificates from the CA file to the pool
//     if !rootCAs.AppendCertsFromPEM(caPEM) {
//         _ = level.Warn(logger).Log("msg", "No certificates were parsed from the custom CA file")
//     }

//     // Log the custom CA certificates
//     for len(caPEM) > 0 {
//         var block *pem.Block
//         block, caPEM = pem.Decode(caPEM)
//         if block == nil {
//             break
//         }
//         if block.Type == "CERTIFICATE" {
//             cert, err := x509.ParseCertificate(block.Bytes)
//             if err == nil {
//                 _ = level.Debug(logger).Log("msg", "Loaded CA certificate",
//                     "subject", cert.Subject,
//                     "issuer", cert.Issuer,
//                     "is_ca", cert.IsCA)
//             }
//         }
//     }

//     // Create and return TLS config
//     return &tls.Config{
//         Certificates: []tls.Certificate{cert},
//         RootCAs:      rootCAs,
//         MinVersion:   tls.VersionTLS12,
//         ServerName:   "osprey-dev-us-east.us-east.serviceendpoint.cloud.ibm.com",
//     }, nil
// }

// NewOspreyClient creates an OspreyClient with the given configuration.
func NewOspreyClient(cfg OspreyClientConfig, logger log.Logger) (*OspreyClient, error) {
	checkEnvVars()

	if cfg.QueryCertFile == "" || cfg.QueryKeyFile == "" {
		return nil, fmt.Errorf("missing query cert/key: required")
	}
	if cfg.AlertsCertFile == "" || cfg.AlertsKeyFile == "" {
		return nil, fmt.Errorf("missing alerts cert/key: required")
	}

	ingestCert := cfg.IngestCertFile
	ingestKey := cfg.IngestKeyFile
	if ingestCert == "" || ingestKey == "" {
		ingestCert = cfg.QueryCertFile
		ingestKey = cfg.QueryKeyFile
	}

	// First, update the makeTLS function to include the ServerName
	makeTLS := func(certFile, keyFile, caFile string) (*tls.Config, error) {
		// Load client certificate
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert %s: %w", certFile, err)
		}

		return &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, // Skip certificate verification
			MinVersion:         tls.VersionTLS12,
			ServerName:         "osprey-dev-us-east.us-east.serviceendpoint.cloud.ibm.com", // Important for SNI
		}, nil
	}

	// Then update the HTTP transport creation to use the TLS config
	ingTLS, err := makeTLS(ingestCert, ingestKey, cfg.CACertFile)
	if err != nil {
		return nil, err
	}
	qryTLS, err := makeTLS(cfg.QueryCertFile, cfg.QueryKeyFile, cfg.CACertFile)
	if err != nil {
		return nil, err
	}
	amTLS, err := makeTLS(cfg.AlertsCertFile, cfg.AlertsKeyFile, cfg.CACertFile)
	if err != nil {
		return nil, err
	}

	// Create custom transports with the TLS config
	ingRT := &http.Transport{
		TLSClientConfig: ingTLS,
		// Add these settings for better connection handling
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	qryRT := &http.Transport{
		TLSClientConfig: qryTLS,
		MaxIdleConns:    100,
		IdleConnTimeout: 90 * time.Second,
	}
	amRT := &http.Transport{
		TLSClientConfig: amTLS,
		MaxIdleConns:    100,
		IdleConnTimeout: 90 * time.Second,
	}

	type ClientRoundTripper struct {
		Rt                http.RoundTripper
		TenantID          string
		BasicAuthUser     string
		BasicAuthPassword string
		BearerToken       string
		RequestDebug      bool
		UserAgent         string
	}

	ingWrapped := &clientRoundTripper{
		Ct: continuoustest.ClientRoundTripper{
			Rt:                ingRT,
			TenantID:          cfg.Ct.TenantID,
			BasicAuthUser:     cfg.Ct.BasicAuthUser,
			BasicAuthPassword: cfg.Ct.BasicAuthPassword,
			BearerToken:       cfg.Ct.BearerToken,
			RequestDebug:      cfg.Ct.RequestDebug,
			UserAgent:         cfg.Ct.UserAgent,
		},
		WriteBaseEndpoint: cfg.Ct.WriteBaseEndpoint,
		ReadBaseEndpoint:  cfg.Ct.ReadBaseEndpoint,
		SourceTenants:     cfg.SourceTenants,
	}

	qryWrapped := &clientRoundTripper{
		Ct: continuoustest.ClientRoundTripper{
			Rt:                qryRT,
			TenantID:          cfg.Ct.TenantID,
			BasicAuthUser:     cfg.Ct.BasicAuthUser,
			BasicAuthPassword: cfg.Ct.BasicAuthPassword,
			BearerToken:       cfg.Ct.BearerToken,
			RequestDebug:      cfg.Ct.RequestDebug,
			UserAgent:         cfg.Ct.UserAgent,
		},
		WriteBaseEndpoint: cfg.Ct.WriteBaseEndpoint,
		ReadBaseEndpoint:  cfg.Ct.ReadBaseEndpoint,
		SourceTenants:     cfg.SourceTenants,
	}

	amWrapped := &clientRoundTripper{Ct: continuoustest.ClientRoundTripper{Rt: amRT,
		TenantID:          cfg.Ct.TenantID,
		BasicAuthUser:     cfg.Ct.BasicAuthUser,
		BasicAuthPassword: cfg.Ct.BasicAuthPassword,
		BearerToken:       cfg.Ct.BearerToken,
		RequestDebug:      cfg.Ct.RequestDebug,
		UserAgent:         cfg.Ct.UserAgent},
		WriteBaseEndpoint: cfg.Ct.WriteBaseEndpoint,
		ReadBaseEndpoint:  cfg.Ct.ReadBaseEndpoint,
		SourceTenants:     cfg.SourceTenants,
	}

	writeHTTP := &http.Client{Transport: ingWrapped}
	mgmtHTTP := &http.Client{Transport: amWrapped}
	address := cfg.Ct.ReadBaseEndpoint.String()
	if cfg.ReadPromBaseEndpoint.URL != nil {
		address = cfg.ReadPromBaseEndpoint.String()
	}
	apiCfg := api.Config{Address: address, RoundTripper: qryWrapped}
	readClient, err := api.NewClient(apiCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create read client: %w", err)
	}
	if cfg.Ct.WriteBaseEndpoint.URL == nil {
		return nil, errors.New("the write endpoint has not been set")
	}
	if cfg.Ct.ReadBaseEndpoint.URL == nil {
		return nil, errors.New("the read endpoint has not been set")
	}
	if cfg.Ct.WriteProtocol != "prometheus" && cfg.Ct.WriteProtocol != "otlp-http" {
		return nil, fmt.Errorf("the only supported write protocols are \"prometheus\" or \"otlp-http\"")
	}

	if (cfg.Ct.TenantID != defaultTenant && cfg.Ct.BasicAuthUser != "" && cfg.Ct.BasicAuthPassword != "" && cfg.Ct.BearerToken != "") || (cfg.Ct.BasicAuthUser != "" && cfg.Ct.BasicAuthPassword != "" && cfg.Ct.BearerToken != "") {
		return nil, errors.New("either set tests.tenant-id or tests.basic-auth-user/tests.basic-auth-password or tests.bearer-token")
	}

	var writeClient continuoustest.ClientWriter
	switch cfg.Ct.WriteProtocol {
	case "prometheus":
		writeClient = &continuoustest.PrometheusWriter{HttpClient: writeHTTP, WriteBaseEndpoint: cfg.Ct.WriteBaseEndpoint,
			WriteBatchSize: cfg.Ct.WriteBatchSize, WriteTimeout: cfg.Ct.WriteTimeout}
	case "otlp-http":
		writeClient = &continuoustest.OtlpHTTPWriter{
			HttpClient:        writeHTTP,
			WriteBaseEndpoint: cfg.Ct.WriteBaseEndpoint,
			WriteBatchSize:    cfg.Ct.WriteBatchSize,
			WriteTimeout:      cfg.Ct.WriteTimeout,
		}
	default:
		return nil, fmt.Errorf("unsupported write protocol: %s", cfg.Ct.WriteProtocol)
	}
	// cc := continuoustest.Client{
	//  WriteClient: writeClient,
	//  ReadClient:  v1.NewAPI(readClient),
	//  Logger:      logger,
	// }
	return &OspreyClient{
		WriteClient: writeClient,
		ReadClient:  v1.NewAPI(readClient),
		Logger:      logger,
		Cfg:         cfg,
		httpClient:  mgmtHTTP,
		// Client:     ,
		// httpClient: mgmtHTTP,
	}, nil
	// Wrap transports
	// ingWrapped := &clientRoundTripper{rt: &http.Transport{TLSClientConfig: ingTLS}, tenantID: cfg.Ct.TenantID, basicAuthUser: cfg.Ct.BasicAuthUser, basicAuthPassword: cfg.Ct.BasicAuthPassword, bearerToken: cfg.Ct.BearerToken, userAgent: cfg.Ct.UserAgent, requestDebug: cfg.Ct.RequestDebug, writeBaseEndpoint: cfg.Ct.WriteBaseEndpoint, ReadBaseEndpoint: cfg.Ct.ReadBaseEndpoint, SourceTenants: cfg.SourceTenants}
	// qryWrapped := &clientRoundTripper{rt: &http.Transport{TLSClientConfig: qryTLS}, tenantID: cfg.Ct.TenantID, basicAuthUser: cfg.Ct.BasicAuthUser, basicAuthPassword: cfg.Ct.BasicAuthPassword, bearerToken: cfg.Ct.BearerToken, userAgent: cfg.Ct.UserAgent, requestDebug: cfg.Ct.RequestDebug, writeBaseEndpoint: cfg.Ct.WriteBaseEndpoint, ReadBaseEndpoint: cfg.Ct.ReadBaseEndpoint, SourceTenants: cfg.SourceTenants}
	// amWrapped := &clientRoundTripper{rt: &http.Transport{TLSClientConfig: amTLS}, tenantID: cfg.Ct.TenantID, basicAuthUser: cfg.Ct.BasicAuthUser, basicAuthPassword: cfg.Ct.BasicAuthPassword, bearerToken: cfg.Ct.BearerToken, userAgent: cfg.Ct.UserAgent, requestDebug: cfg.Ct.RequestDebug, writeBaseEndpoint: cfg.Ct.WriteBaseEndpoint, ReadBaseEndpoint: cfg.Ct.ReadBaseEndpoint, SourceTenants: cfg.SourceTenants}

	// cfg.WriteTransport = ingWrapped
	// cfg.ReadTransport = qryWrapped

	// // HTTP client for management operations
	// mgmtHTTP := &http.Client{Transport: amWrapped}

	// // Create continuous client
	// baseClient, err := continuoustest.NewClient(cfg.Ct, logger)
	// if err != nil {
	//  return nil, fmt.Errorf("failed to create base continuous client: %w", err)
	// }

	// return &OspreyClient{
	//  Client:     baseClient,
	//  httpClient: mgmtHTTP,
	// }, nil
}

// SetAlertmanagerConfig pushes an alertmanager YAML config to the cluster via the management API.
func (c *OspreyClient) SetAlertmanagerConfig(ctx context.Context, alertmanagerYAML string, templateFiles map[string]string) error {
	type payload struct {
		TemplateFiles      map[string]string `yaml:"template_files" json:"template_files"`
		AlertmanagerConfig string            `yaml:"alertmanager_config" json:"alertmanager_config"`
	}
	body := []byte(fmt.Sprintf("alertmanager_config: |\n%s", indentYAML(alertmanagerYAML, 2)))

	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Cfg.Ct.ReadBaseEndpoint.String()+"/api/v1/alerts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/yaml")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusAccepted {
		truncatedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrMsgLen))
		return fmt.Errorf("alertmanager config push failed: %s, body=%q", resp.Status, string(truncatedBody))
	}
	return nil
}

// CreateRuleGroup pushes a Prometheus rule group YAML into a namespace.
func (c *OspreyClient) CreateRuleGroup(ctx context.Context, namespace string, groupYAML string) error {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Cfg.Ct.ReadBaseEndpoint.String()+"/prometheus/config/v1/rules/"+namespace, bytes.NewReader([]byte(groupYAML)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/yaml")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusAccepted {
		truncatedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrMsgLen))
		return fmt.Errorf("rule group push failed: %s, body=%q", resp.Status, string(truncatedBody))
	}
	return nil
}

// DeleteRuleNamespace deletes an entire Prometheus rule namespace.
func (c *OspreyClient) DeleteRuleNamespace(ctx context.Context, namespace string) error {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.Cfg.Ct.ReadBaseEndpoint.String()+"/prometheus/config/v1/rules/"+namespace, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		truncatedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrMsgLen))
		return fmt.Errorf("rule namespace delete failed: %s, body=%q", resp.Status, string(truncatedBody))
	}
	return nil
}

// ListActiveAlerts returns alerts (name + state + labels) and the HTTP status code.
func (c *OspreyClient) ListActiveAlerts(ctx context.Context) ([]Alert, int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	base := c.Cfg.Ct.ReadBaseEndpoint.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/prometheus/api/v1/alerts", nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	if status/100 != 2 {
		truncatedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrMsgLen))
		return nil, status, fmt.Errorf("alerts list failed: %s, body=%q", resp.Status, string(truncatedBody))
	}
	type response struct {
		Status string `json:"status"`
		Data   struct {
			Alerts []struct {
				Labels map[string]string `json:"labels"`
				State  string            `json:"state"`
			} `json:"alerts"`
		} `json:"data"`
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, status, err
	}
	var out response
	if err := jsonUnmarshal(b, &out); err != nil {
		return nil, status, err
	}
	var alerts []Alert
	for _, a := range out.Data.Alerts {
		alerts = append(alerts, Alert{
			Name:   a.Labels["alertname"],
			State:  a.State,
			Labels: a.Labels,
		})
	}
	return alerts, status, nil
}

// GetAlertmanagerConfig reads the alertmanager config body as string and returns it with the HTTP status.
func (c *OspreyClient) GetAlertmanagerConfig(ctx context.Context) (string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Cfg.Ct.ReadBaseEndpoint.String()+"/api/v1/alerts", nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", status, err
	}
	if status/100 != 2 {
		return "", status, fmt.Errorf("get alertmanager config failed: %s, body=%q", resp.Status, string(b))
	}
	return string(b), status, nil
}

// DeleteAlertmanagerConfig deletes the current alertmanager config.
func (c *OspreyClient) DeleteAlertmanagerConfig(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.Cfg.Ct.ReadBaseEndpoint.String()+"/api/v1/alerts", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		truncatedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrMsgLen))
		return fmt.Errorf("delete alertmanager config failed: %s, body=%q", resp.Status, string(truncatedBody))
	}
	return nil
}

// GetRuleNamespace returns the YAML for a given rule namespace.
func (c *OspreyClient) GetRuleNamespace(ctx context.Context, namespace string) (string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.Cfg.Ct.ReadTimeout)
	defer cancel()
	urlStr := fmt.Sprintf("%s/prometheus/config/v1/rules/%s", c.Cfg.Ct.ReadBaseEndpoint.String(), urlPathEscape(namespace))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", status, err
	}
	if status/100 != 2 {
		return "", status, fmt.Errorf("get rule namespace failed: %s, body=%q", resp.Status, string(b))
	}
	return string(b), status, nil
}

// indentYAML indents each non-empty line by n spaces.
func indentYAML(s string, n int) string {
	pad := bytes.Repeat([]byte(" "), n)
	lines := bytes.Split([]byte(s), []byte("\n"))
	for i := range lines {
		if len(lines[i]) > 0 {
			lines[i] = append(pad, lines[i]...)
		}
	}
	return string(bytes.Join(lines, []byte("\n")))
}

// jsonUnmarshal is a small wrapper around json.Unmarshal so it can be mocked/overridden in tests (kept simple here).
func jsonUnmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

// urlPathEscape escapes a path segment for safe inclusion in a URL path.
func urlPathEscape(s string) string {
	return (&url.URL{Path: s}).EscapedPath()
}
