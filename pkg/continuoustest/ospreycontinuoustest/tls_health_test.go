package ospreycontinuoustest

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"

	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	agent "github.com/grafana/mimir/pkg/continuoustest"
	"github.com/stretchr/testify/require"
)

type envConfig struct {
	writeEndpoint string
	readEndpoint  string
	tenantID      string
	caFile        string
	queryCert     string
	queryKey      string
}

func loadCommonEnv(t *testing.T) envConfig {
	t.Helper()

	cfg := envConfig{
		writeEndpoint: os.Getenv("WRITE_BASE_ENDPOINT"),
		readEndpoint:  os.Getenv("READ_BASE_ENDPOINT"),
		tenantID:      os.Getenv("TENANT_ID"),
		caFile:        os.Getenv("CA_CERT_FILE"),
		queryCert:     os.Getenv("QUERY_CERT_FILE"),
		queryKey:      os.Getenv("QUERY_KEY_FILE"),
	}

	if cfg.readEndpoint == "" || cfg.tenantID == "" || cfg.caFile == "" ||
		cfg.queryCert == "" || cfg.queryKey == "" {
		t.Skip("required env vars not set")
	}

	return cfg
}

// TestTlsHealthTestCases runs TLS handshake tests against different client certificates.
func TestTlsHealthTestCases(t *testing.T) {
	cfg := loadCommonEnv(t)

	httpsServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer httpsServer.Close()

	httpsServer.TLS = &tls.Config{}
	httpsServer.StartTLS()

	type certCase struct {
		name          string
		certEnv       string
		keyEnv        string
		expectSuccess bool
	}

	testCases := []certCase{
		{
			name:          "Ingestion with invalid Certificate (role xyz)",
			certEnv:       "CERT_INVALID_ROLE",
			keyEnv:        "KEY_INVALID_ROLE",
			expectSuccess: false, // set true/false based on whether TLS should pass for this cert
		},
		{
			name:          "Ingestion with Ingestion role Certificate",
			certEnv:       "CERT_INGEST_ROLE",
			keyEnv:        "KEY_INGEST_ROLE",
			expectSuccess: true,
		},
		{
			name:          "Ingestion with Query role Certificate",
			certEnv:       "CERT_QUERY_ROLE",
			keyEnv:        "KEY_QUERY_ROLE",
			expectSuccess: false, // again, TLS vs auth depends on your server behaviour
		},
		{
			name:          "Ingestion with Ingestion role Cert, invalid supertenant, valid tenant",
			certEnv:       "CERT_INGEST_ROLE_INVALID_SUPERTENANT",
			keyEnv:        "KEY_INGEST_ROLE_INVALID_SUPERTENANT",
			expectSuccess: true, // per your table: TLS connects and ingestion happens
		},
		{
			name:          "Ingestion with Expired Certificate",
			certEnv:       "CERT_EXPIRED",
			keyEnv:        "KEY_EXPIRED",
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ingestCert := os.Getenv(tc.certEnv)
			ingestKey := os.Getenv(tc.keyEnv)
			if ingestCert == "" || ingestKey == "" {
				t.Skipf("env vars %s/%s not set; skipping", tc.certEnv, tc.keyEnv)
			}
			clientCfg := OspreyClientConfig{
				Ct: agent.ClientConfig{ // Use ClientConfig, not agent.Config
					WriteBaseEndpoint: parseURL(t, cfg.writeEndpoint),
					ReadBaseEndpoint:  parseURL(t, cfg.readEndpoint),
					TenantID:          cfg.tenantID,
				},
				CACertFile:     cfg.caFile,
				IngestCertFile: ingestCert,
				IngestKeyFile:  ingestKey,
				QueryCertFile:  cfg.queryCert,
				QueryKeyFile:   cfg.queryKey,
				AlertsCertFile: cfg.queryCert,
				AlertsKeyFile:  cfg.queryKey,
			}

			tlsTest := NewTlsHealthTest(TlsHealthTestConfig{
				Enabled:     true,
				CheckIngest: true,
				CheckQuery:  true,
				CheckAlerts: false,
				DialTimeout: 5 * time.Second,
			}, clientCfg, log.NewNopLogger())

			err := tlsTest.Run(context.Background(), time.Now())
			if tc.expectSuccess {
				require.NoError(t, err, "expected TLS handshake success")
			} else {
				require.Error(t, err, "expected TLS handshake failure")
			}
		})
	}
}

func parseURL(t *testing.T, raw string) flagext.URLValue {
	t.Helper()
	var v flagext.URLValue
	require.NoError(t, v.Set(raw))
	return v
}
