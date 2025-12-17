// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/tracing"
	"github.com/grafana/mimir/pkg/continuoustest"

	osprey "github.com/grafana/mimir/pkg/continuoustest/ospreycontinuoustest"
	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"gopkg.in/yaml.v3"
)

type testResult struct {
	Name   string
	Passed bool
	ErrMsg string
}

func main() {
	// Parse CLI arguments.
	// cfg := &continuoustest.Config{}
	ospreyCfg := osprey.OspreyConfig{}
	var (
		serverMetricsPort int
		logLevel          log.Level
	)
	flag.CommandLine.IntVar(&serverMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	ospreyCfg.RegisterFlags(flag.CommandLine)
	logLevel.RegisterFlags(flag.CommandLine)
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	util_log.InitLogger(log.LogfmtFormat, logLevel, false, util_log.RateLimitedLoggerCfg{})
	level.Warn(util_log.Logger).Log("msg", "The mimir-continuous-test binary you are using is deprecated. Please use the Mimir binary module `mimir -target=continuous-test`.")
	// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
	if trace, err := tracing.NewFromEnv("mimir-continuous-test", jaegercfg.MaxTagValueLength(16e3)); err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
	} else {
		defer trace.Close()
	}
	logger := util_log.Logger
	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(version.NewCollector("mimir_continuous_test"))
	registry.MustRegister(collectors.NewGoCollector())
	i := instrumentation.NewMetricsServer(serverMetricsPort, registry, util_log.Logger)
	if err := i.Start(); err != nil {
		level.Error(logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	if ospreyCfg.EnvConfigPath != "" {
		apiKey := os.Getenv(ospreyCfg.IBMAPIKeyEnv)
		if apiKey == "" {
			level.Error(logger).Log("msg", "IBM API key env var is empty", "env_var", ospreyCfg.IBMAPIKeyEnv)
			util_log.Flush()
			os.Exit(1)
		}
		ef, err := loadEnvFile(ospreyCfg.EnvConfigPath)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to load env-config", "err", err.Error())
			util_log.Flush()
			os.Exit(1)
		}
		envs := selectEnvs(ef.Environments, ospreyCfg.EnvSelect)
		anyErr := false
		for _, e := range envs {
			level.Info(logger).Log("msg", "Running environment", "env", e.Name)
			cc, err := buildClientConfigFromEnv(context.Background(), osprey.OspreyClientConfig{}, e, ospreyCfg.IAMEndpoint, apiKey, logger)
			if err != nil {
				level.Error(logger).Log("msg", "Build client config failed", "env", e.Name, "err", err.Error())
				anyErr = true
				continue
			}
			level.Info(logger).Log("msg", "Creating Osprey client", "endpoint\n", cc.Ct.WriteBaseEndpoint.String())
			ospreyclient, err := osprey.NewOspreyClient(cc, logger)
			if err != nil {
				// level.Debug("Failed to initialize client",err)
				// util_log.Debug("Failed to initialize client", err)
				level.Error(logger).Log("msg", "Failed to initialize client", "env", e.Name, "err", err.Error())
				anyErr = true
				continue
			}
			if cc.CACertFile != "" {
				level.Debug(logger).Log("msg", "Using custom CA certificate", "path", cc.CACertFile)
			} else {
				level.Debug(logger).Log("msg", "Using system CA certificates")
			}
			level.Info(logger).Log("msg", "Creating continuous test client", "write_endpoint", cc.Ct.WriteBaseEndpoint.String(), "read_endpoint", cc.Ct.ReadBaseEndpoint.String())

			// Inside the environment loop (preferred approach)
			client, err := continuoustest.NewClient(cc.Ct, logger)
			if err != nil {
				// util_log.Debug("Failed to initialize client", err)
				level.Error(logger).Log("msg", "Failed to initialize client",
					"env", e.Name,
					"error", err,
					"write_endpoint", cc.Ct.WriteBaseEndpoint.String(),
					"read_endpoint", cc.Ct.ReadBaseEndpoint.String(),
					"write_protocol", cc.Ct.WriteProtocol)
				anyErr = true
				continue
			}

			envRegistry := prometheus.NewRegistry()

			mgrCfg := ospreyCfg.Ct.Manager
			mgrCfg.SmokeTest = true
			m := continuoustest.NewManager(mgrCfg, logger)
			clientCfg := ospreyCfg.Ct.Client
			if ospreyCfg.EnvConfigPath == "" {
				if writeEndpoint := os.Getenv("WRITE_BASE_ENDPOINT"); writeEndpoint != "" {
					if err := clientCfg.WriteBaseEndpoint.Set(writeEndpoint); err != nil {
						level.Error(logger).Log("msg", "Invalid write endpoint URL", "err", err)
						os.Exit(1)
					}
				}

				if readEndpoint := os.Getenv("READ_BASE_ENDPOINT"); readEndpoint != "" {
					if err := clientCfg.ReadBaseEndpoint.Set(readEndpoint); err != nil {
						level.Error(logger).Log("msg", "Invalid read endpoint URL", "err", err)
						os.Exit(1)
					}
				}
			}
			var results []testResult // Use the package-level type

			m.SetOnTestComplete(func(name string, passed bool, err error) {
				r := testResult{Name: name, Passed: passed}
				if err != nil {
					em := err.Error()
					if len(em) > 5000 {
						em = em[:5000] + "…"
					}
					r.ErrMsg = em
				}
				results = append(results, r)
			})
			// TLS health check (cert incorrect) runs first if enabled
			if ospreyCfg.TLSHealthTest.Enabled {
				osprey.NewTlsHealthTest(ospreyCfg.TLSHealthTest, cc, logger)
			}

			// envRegistry := prometheus.NewRegistry()
			m.AddTest(continuoustest.NewWriteReadSeriesTest(ospreyCfg.Ct.WriteReadSeriesTest, client, logger, envRegistry))
			// m.AddTest(continuoustest.NewWriteReadSeriesTest(ospreyCfg.Ct.WriteReadSeriesTest, ospreyclient.Client, logger, envRegistry))
			//  envRegistry := prometheus.NewRegistry()
			if ospreyCfg.AlertingTest.Enabled {
				m.AddTest(osprey.NewAlertingTest(ospreyCfg.AlertingTest, continuoustest.WriteReadSeriesTestConfig{}, ospreyclient, logger, envRegistry))
			}
			if err := m.Run(context.Background()); err != nil {
				if errors.Is(err, modules.ErrStopProcess) {
					level.Info(logger).Log("msg", "All tests completed (stop process)", "env", e.Name)
				} else {
					level.Info(logger).Log("msg", "Continuous test failed", "env", e.Name, "err", err)
					anyErr = true
				}
			} else {
				level.Info(logger).Log("msg", "All Test passed", "env", e.Name)
			}

			sendSlackNotificationIfConfigured(context.Background(), buildSlackSummary(results, e.Name), logger)
		}
		util_log.Flush()
		if anyErr {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// client, err := continuoustest.NewClient(cfg.Client, logger)
	ospreyclientcc, err := osprey.NewOspreyClient(ospreyCfg.Occ, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to initialize client ospreyy", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}
	// Run continuous testing.
	mgrCfg := ospreyCfg.Ct.Manager
	mgrCfg.SmokeTest = true
	// m := continuoustest.NewManager(mgrCfg, logger)

	m := continuoustest.NewManager(mgrCfg, logger)
	clientCfg := ospreyCfg.Ct.Client
	var results []testResult // Use the package-level type
	if writeEndpoint := os.Getenv("WRITE_BASE_ENDPOINT"); writeEndpoint != "" {
		if err := clientCfg.WriteBaseEndpoint.Set(writeEndpoint); err != nil {
			level.Error(logger).Log("msg", "Invalid write endpoint URL", "err", err)
			os.Exit(1)
		}
	}

	if readEndpoint := os.Getenv("READ_BASE_ENDPOINT"); readEndpoint != "" {
		if err := clientCfg.ReadBaseEndpoint.Set(readEndpoint); err != nil {
			level.Error(logger).Log("msg", "Invalid read endpoint URL", "err", err)
			os.Exit(1)
		}
	}

	// Create the client with the configured endpoints
	client, err := continuoustest.NewClient(clientCfg, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to initialize client", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}
	m.SetOnTestComplete(func(name string, passed bool, err error) {
		r := testResult{Name: name, Passed: passed}
		if err != nil {
			em := err.Error()
			if len(em) > 5000 {
				em = em[:5000] + "…"
			}
			r.ErrMsg = em
		}
		results = append(results, r)
	})
	if ospreyCfg.TLSHealthTest.Enabled {
		// tlsClientCfg := osprey.OspreyClientConfig{
		//  Ct:             clientCfg,
		//  // CACertFile:     ospreyCfg.CACertFile,
		//  // ClientCertFile: ospreyCfg.ClientCertFile,
		//  // ClientKeyFile:  ospreyCfg.ClientKeyFile,
		// }
		m.AddTest(osprey.NewTlsHealthTest(ospreyCfg.TLSHealthTest, ospreyCfg.Occ, logger))
	}
	m.AddTest(continuoustest.NewWriteReadSeriesTest(ospreyCfg.Ct.WriteReadSeriesTest, client, logger, registry))
	if ospreyCfg.AlertingTest.Enabled {
		m.AddTest(osprey.NewAlertingTest(ospreyCfg.AlertingTest, ospreyCfg.Ct.WriteReadSeriesTest, ospreyclientcc, logger, registry))
	}
	if err := m.Run(context.Background()); err != nil {
		if !errors.Is(err, modules.ErrStopProcess) {
			level.Error(logger).Log("msg", "Failed to run continuous test", "err", err.Error())
			util_log.Flush()
			sendSlackNotificationIfConfigured(context.Background(), buildSlackSummary(results, ""), logger)
			os.Exit(1)
		}
		util_log.Flush()
		sendSlackNotificationIfConfigured(context.Background(), buildSlackSummary(results, ""), logger)
	}
}

type Env struct {
	Name               string `yaml:"name"`
	WriteEndpoint      string `yaml:"write_endpoint"`
	ReadEndpoint       string `yaml:"read_endpoint"`
	ReadPromEndpoint   string `yaml:"read_prom_endpoint"`
	TenantID           string `yaml:"tenant_id"`
	SourceTenants      string `yaml:"source_tenants_header"`
	SecretsManagerURL  string `yaml:"secrets_manager_url"`
	ContinuousSecretID string `yaml:"continuous_test_secret_id"`
	AlertSecretID      string `yaml:"alert_secret_id"`
	IngestCertFile     string `yaml:"ingest_cert"`
	IngestKeyFile      string `yaml:"ingest_key"`
	QueryCertFile      string `yaml:"query_cert"`
	QueryKeyFile       string `yaml:"query_key"`
	AlertsCertFile     string `yaml:"alerts_cert"`
	AlertsKeyFile      string `yaml:"alerts_key"`
	CACertFile         string `yaml:"ca_cert"`
}
type EnvFile struct {
	Environments []Env `yaml:"environments"`
}

func loadEnvFile(path string) (*EnvFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ef EnvFile
	if err := yaml.Unmarshal(b, &ef); err != nil {
		return nil, err
	}
	return &ef, nil
}
func selectEnvs(all []Env, selectExpr string) []Env {
	if selectExpr == "" || strings.EqualFold(selectExpr, "all") {
		return all
	}
	tokens := []string{}
	for _, n := range strings.Split(selectExpr, ",") {
		t := strings.TrimSpace(n)
		if t != "" {
			tokens = append(tokens, t)
		}
	}
	isGroupToken := func(tok string) (string, bool) {
		low := strings.ToLower(tok)
		switch low {
		case "dev", "stage", "prod":
			return low, true
		default:
			return "", false
		}
	}
	selected := make(map[string]struct{})
	var out []Env
	for _, e := range all {
		nameLow := strings.ToLower(e.Name)
		for _, tok := range tokens {
			// exact match by name
			if strings.EqualFold(e.Name, tok) {
				if _, seen := selected[e.Name]; !seen {
					selected[e.Name] = struct{}{}
					out = append(out, e)
				}
				continue
			}
			// group token match by suffix (-dev|-stage|-prod)
			if grp, ok := isGroupToken(tok); ok {
				if strings.HasSuffix(nameLow, "-"+grp) {
					if _, seen := selected[e.Name]; !seen {
						selected[e.Name] = struct{}{}
						out = append(out, e)
					}
				}
			}
		}
	}
	return out
}

type certBundle struct {
	Certificate string
	PrivateKey  string
	IssuingCA   string
}

func getIAMToken(ctx context.Context, iamEndpoint, apiKey string) (string, error) {
	form := url.Values{}
	form.Set("grant_type", "urn:ibm:params:cry:grant-type:apikey")
	form.Set("apikey", apiKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, iamEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("iam token error: %s: %s", resp.Status, string(b))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.AccessToken == "" {
		return "", errors.New("empty access_token from IAM")
	}
	return out.AccessToken, nil
}
func fetchSecretBundle(ctx context.Context, secretsManagerURL, secretID, bearer string) (certBundle, error) {
	var bundle certBundle
	u := strings.TrimRight(secretsManagerURL, "/") + "/api/v2/secrets/" + url.PathEscape(secretID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return bundle, err
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return bundle, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return bundle, fmt.Errorf("secrets manager error: %s: %s", resp.Status, string(b))
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return bundle, err
	}
	getStr := func(m map[string]any, k string) string {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	bundle.Certificate = getStr(raw, "certificate")
	bundle.PrivateKey = getStr(raw, "private_key")
	bundle.IssuingCA = getStr(raw, "issuing_ca")
	if bundle.Certificate == "" || bundle.PrivateKey == "" || bundle.IssuingCA == "" {
		if res, ok := raw["resources"].([]any); ok && len(res) > 0 {
			if m0, ok := res[0].(map[string]any); ok {
				if sd, ok := m0["secret_data"].(map[string]any); ok {
					if bundle.Certificate == "" {
						bundle.Certificate = getStr(sd, "certificate")
					}
					if bundle.PrivateKey == "" {
						bundle.PrivateKey = getStr(sd, "private_key")
					}
					if bundle.IssuingCA == "" {
						bundle.IssuingCA = getStr(sd, "issuing_ca")
					}
				}
			}
		}
	}
	if bundle.Certificate == "" || bundle.PrivateKey == "" || bundle.IssuingCA == "" {
		return bundle, errors.New("missing certificate/private_key/issuing_ca in secret")
	}
	return bundle, nil
}

func writeBundle(destDir, prefix string, b certBundle) (certPath, keyPath, caPath string, err error) {
	if err = os.MkdirAll(destDir, 0o755); err != nil {
		return
	}
	certPath = filepath.Join(destDir, prefix+"_client.crt")
	keyPath = filepath.Join(destDir, prefix+"_client.key")
	caPath = filepath.Join(destDir, prefix+"_combined_ca.crt")
	if err = os.WriteFile(certPath, []byte(b.Certificate), 0o600); err != nil {
		return
	}
	if err = os.WriteFile(keyPath, []byte(b.PrivateKey), 0o600); err != nil {
		return
	}
	if err = os.WriteFile(caPath, []byte(b.IssuingCA), 0o600); err != nil {
		return
	}
	return
}

func buildClientConfigFromEnv(
	ctx context.Context,
	base osprey.OspreyClientConfig,
	e Env,
	iamEndpoint, apiKey string,
	logger gokitlog.Logger,
) (osprey.OspreyClientConfig, error) {

	// Work on a copy
	cc := base

	// --- Set Write & Read Base Endpoints ---
	var w, r, rp flagext.URLValue

	if err := w.Set(e.WriteEndpoint); err != nil {
		return cc, err
	}
	if err := r.Set(e.ReadEndpoint); err != nil {
		return cc, err
	}

	cc.Ct.WriteBaseEndpoint = w
	cc.Ct.ReadBaseEndpoint = r

	// --- Optional Prometheus Query Endpoint ---
	if e.ReadPromEndpoint != "" {
		if err := rp.Set(e.ReadPromEndpoint); err != nil {
			return cc, err
		}
		cc.ReadPromBaseEndpoint = rp
	}

	// --- Tenant & Sources ---
	cc.Ct.TenantID = e.TenantID
	cc.SourceTenants = e.SourceTenants
	cc.Ct.WriteProtocol = "prometheus"

	// Increase timeouts
	cc.Ct.WriteTimeout = 30 * time.Second
	cc.Ct.ReadTimeout = 120 * time.Second

	// Certs provided from ENV directly
	qCert, qKey := e.QueryCertFile, e.QueryKeyFile
	iCert, iKey := e.IngestCertFile, e.IngestKeyFile
	aCert, aKey := e.AlertsCertFile, e.AlertsKeyFile
	caPath := e.CACertFile

	// -------------------------------------------------------
	// Fetch Continuous (Query/Ingest) Certs
	// -------------------------------------------------------
	if (qCert == "" || qKey == "" || caPath == "") && e.ContinuousSecretID != "" {

		token, err := getIAMToken(ctx, iamEndpoint, apiKey)
		if err != nil {
			return cc, fmt.Errorf("get IAM token: %w", err)
		}

		bundle, err := fetchSecretBundle(ctx, e.SecretsManagerURL, e.ContinuousSecretID, token)
		if err != nil {
			return cc, fmt.Errorf("fetch continuous secret: %w", err)
		}

		dest := filepath.Join(os.TempDir(), "mimir-ct", e.Name, "continuous")

		cert, key, ca, err := writeBundle(dest, "continuous", bundle)
		if err != nil {
			return cc, err
		}

		qCert, qKey = cert, key
		if caPath == "" {
			caPath = ca
		}

		// Ingest reuses query certs if not explicitly set
		if iCert == "" || iKey == "" {
			iCert, iKey = cert, key
		}
	}

	// -------------------------------------------------------
	// Fetch Alerting Certs
	// -------------------------------------------------------
	if (aCert == "" || aKey == "") && e.AlertSecretID != "" {

		token, err := getIAMToken(ctx, iamEndpoint, apiKey)
		if err != nil {
			return cc, fmt.Errorf("get IAM token (alerts): %w", err)
		}

		bundle, err := fetchSecretBundle(ctx, e.SecretsManagerURL, e.AlertSecretID, token)
		if err != nil {
			return cc, fmt.Errorf("fetch alert secret: %w", err)
		}

		dest := filepath.Join(os.TempDir(), "mimir-ct", e.Name, "alerts")

		cert, key, ca, err := writeBundle(dest, "alerts", bundle)
		if err != nil {
			return cc, err
		}

		aCert, aKey = cert, key

		if caPath == "" {
			caPath = ca
		}
	}

	// -------------------------------------------------------
	// Assign final cert paths to ClientConfig
	// -------------------------------------------------------
	cc.QueryCertFile = qCert
	cc.QueryKeyFile = qKey

	cc.IngestCertFile = iCert
	cc.IngestKeyFile = iKey

	cc.AlertsCertFile = aCert
	cc.AlertsKeyFile = aKey

	cc.CACertFile = caPath

	logger.Log("msg", "Final certificate paths",
		"query_cert", cc.QueryCertFile,
		"query_key", cc.QueryKeyFile,
		"ingest_cert", cc.IngestCertFile,
		"ingest_key", cc.IngestKeyFile,
		"alerts_cert", cc.AlertsCertFile,
		"alerts_key", cc.AlertsKeyFile,
		"ca_cert", cc.CACertFile)

	return cc, nil
}

func sendSlackNotificationIfConfigured(ctx context.Context, message string, logger gokitlog.Logger) {
	webhook := os.Getenv("SLACK_WEBHOOK_URL")
	if webhook == "" {
		return
	}
	body, err := json.Marshal(map[string]string{"text": message})
	if err != nil {
		level.Error(logger).Log("msg", "Failed to marshal Slack payload", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(body))
	if err != nil {
		level.Error(logger).Log("msg", "Failed to create Slack request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to send Slack request", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		level.Error(logger).Log("msg", "Slack notification failed", "status_code", resp.StatusCode)
		return
	}
	level.Debug(logger).Log("msg", "Slack notification sent", "status_code", resp.StatusCode)
}

// buildSlackSummary now accepts a slice of the named type testResult.
func buildSlackSummary(results []testResult, envName string) string {
	allPassed := true
	var b strings.Builder
	for _, r := range results {
		if !r.Passed {
			allPassed = false
		}
	}
	if allPassed {
		if envName != "" {
			b.WriteString(":white_check_mark: Mimir continuous test summary in `" + envName + "`\n")
		} else {
			b.WriteString(":white_check_mark: Mimir continuous test summary\n")
		}
	} else {
		if envName != "" {
			b.WriteString(":x: Mimir continuous test summary in `" + envName + "`\n")
		} else {
			b.WriteString(":x: Mimir continuous test summary\n")
		}
	}
	for _, r := range results {
		if r.Passed {
			b.WriteString("- :white_check_mark: " + r.Name + " `passed`\n")
		} else {
			b.WriteString("- :x: " + r.Name + " `failed`\n")
			if r.ErrMsg != "" {
				b.WriteString("> " + r.ErrMsg + "\n")
			}
		}
	}
	return b.String()
}
