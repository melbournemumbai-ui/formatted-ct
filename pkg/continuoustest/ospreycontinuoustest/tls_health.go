package ospreycontinuoustest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// TlsHealthTestConfig controls the certificate/TLS handshake validation test.
type TlsHealthTestConfig struct {
	Enabled     bool
	DialTimeout time.Duration
	CheckIngest bool
	CheckQuery  bool
	CheckAlerts bool
	// CACertFile  string
}

func (cfg *TlsHealthTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.tls-health-test.enabled", false, "Enable TLS/certificate health test.")
	f.DurationVar(&cfg.DialTimeout, "tests.tls-health-test.timeout", 10*time.Second, "TLS handshake timeout.")
	f.BoolVar(&cfg.CheckIngest, "tests.tls-health-test.check-ingest", true, "Validate ingestion endpoint TLS.")
	f.BoolVar(&cfg.CheckQuery, "tests.tls-health-test.check-query", true, "Validate query endpoint TLS.")
	f.BoolVar(&cfg.CheckAlerts, "tests.tls-health-test.check-alerts", true, "Validate alerts/rules endpoint TLS.")
}

// TlsHealthTest validates TLS handshakes against configured endpoints using the provided certs.
type TlsHealthTest struct {
	name      string
	cfg       TlsHealthTestConfig
	clientCfg OspreyClientConfig
	logger    log.Logger
}

func NewTlsHealthTest(cfg TlsHealthTestConfig, clientCfg OspreyClientConfig, logger log.Logger) *TlsHealthTest {
	return &TlsHealthTest{
		name:      "tls-health",
		cfg:       cfg,
		clientCfg: clientCfg,
		logger:    log.With(logger, "test", "tls-health"),
	}
}
func (t *TlsHealthTest) Name() string                                  { return t.name }
func (t *TlsHealthTest) Init(ctx context.Context, now time.Time) error { return nil }
func (t *TlsHealthTest) Run(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}
	if t.clientCfg.CACertFile == "" {
		level.Debug(t.logger).Log("msg", "CA certificate file path", "ca_cert_file", t.clientCfg.CACertFile)
		return errors.New("tests.ca-cert-file is required for TLS health test")
	}
	if t.clientCfg.QueryCertFile == "" || t.clientCfg.QueryKeyFile == "" {
		level.Debug(t.logger).Log("msg", "Query certificate file path", "query_cert_file", t.clientCfg.QueryCertFile)
		return errors.New("tests.query-cert-file and tests.query-key-file are required for TLS health test")
	}
	// helper to build TLS config
	makeTLS := func(certFile, keyFile, caFile string) (*tls.Config, error) {
		leaf, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert %s: %w", certFile, err)
		}
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("append CA failed")
		}
		return &tls.Config{Certificates: []tls.Certificate{leaf}, RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
	}
	// ingest uses specific certs or falls back to query certs
	ingestCert := t.clientCfg.IngestCertFile
	ingestKey := t.clientCfg.IngestKeyFile
	if ingestCert == "" || ingestKey == "" {
		ingestCert = t.clientCfg.QueryCertFile
		ingestKey = t.clientCfg.QueryKeyFile
	}
	ingTLS, err := makeTLS(ingestCert, ingestKey, t.clientCfg.CACertFile)
	if err != nil {
		return err
	}
	qryTLS, err := makeTLS(t.clientCfg.QueryCertFile, t.clientCfg.QueryKeyFile, t.clientCfg.CACertFile)
	if err != nil {
		return err
	}
	amTLS, err := makeTLS(t.clientCfg.AlertsCertFile, t.clientCfg.AlertsKeyFile, t.clientCfg.CACertFile)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(t.cfg.DialTimeout)
	var errs []error
	if t.cfg.CheckIngest {

		if err := dialTLS(deadline, t.clientCfg.Ct.WriteBaseEndpoint.String(), ingTLS, t.logger, "ingest"); err != nil {
			errs = append(errs, err)
		}
	}
	if t.cfg.CheckQuery {
		target := t.clientCfg.Ct.ReadBaseEndpoint.String()
		if t.clientCfg.ReadPromBaseEndpoint.URL != nil {
			target = t.clientCfg.ReadPromBaseEndpoint.String()
		}
		if err := dialTLS(deadline, target, qryTLS, t.logger, "query"); err != nil {
			errs = append(errs, err)
		}
	}
	if t.cfg.CheckAlerts {
		if err := dialTLS(deadline, t.clientCfg.Ct.ReadBaseEndpoint.String(), amTLS, t.logger, "alerts"); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return multiError(errs)
	}
	return nil
}
func dialTLS(deadline time.Time, rawURL string, tlsCfg *tls.Config, logger log.Logger, label string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parse %s url: %w", label, err)
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host = host + ":443"
	}
	d := net.Dialer{Timeout: time.Until(deadline)}
	conn, err := tls.DialWithDialer(&d, "tcp", host, tlsCfg)
	if err != nil {
		level.Warn(logger).Log("msg", "TLS handshake failed", "target", host, "label", label, "err", err)
		return fmt.Errorf("%s TLS handshake failed: %w", label, err)
	}
	_ = conn.Close()
	level.Debug(logger).Log("msg", "TLS handshake OK", "target", host, "label", label)
	return nil
}

type multiError []error

func (m multiError) Error() string {
	if len(m) == 0 {
		return ""
	}
	if len(m) == 1 {
		return m[0].Error()
	}
	s := "multiple errors:"
	for _, e := range m {
		s += "\n - " + e.Error()
	}
	return s
}
