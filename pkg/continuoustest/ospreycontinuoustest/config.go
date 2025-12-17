package ospreycontinuoustest

import (
	"flag"

	"github.com/grafana/mimir/pkg/continuoustest"
)

// Config extends the base continuoustest.Config with osprey-specific configuration.
type OspreyConfig struct {
	Occ           OspreyClientConfig
	Ct            continuoustest.Config // Embed the base config
	TLSHealthTest TlsHealthTestConfig   `yaml:"-"`
	AlertingTest  AlertingTestConfig

	// CACertFile     string `yaml:"ca_cert_file"`
	// ClientCertFile string `yaml:"client_cert_file"`
	// ClientKeyFile  string `yaml:"client_key_file"`
	// QueryCertFile  string `yaml:"query_cert_file"`
	// QueryKeyFile   string `yaml:"query_key_file"`
	// IBM Cloud specific fields
	EnvConfigPath string `yaml:"-"`
	EnvSelect     string `yaml:"-"`
	IAMEndpoint   string `yaml:"-"`
	IBMAPIKeyEnv  string `yaml:"-"`
}

// func NewConfig() *OspreyConfig {
//  return &OspreyConfig{
//      TLSHealthTest: TlsHealthTestConfig{
//          // Set default values here
//          Enabled:     false,
//          DialTimeout: 10 * time.Second,
//          CheckIngest: true,
//          CheckQuery:  true,
//          CheckAlerts: true,
//      },
//  }
// }

func (cfg *OspreyConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Occ.RegisterFlags(f)
	cfg.Ct.Manager.RegisterFlags(f)
	cfg.Ct.WriteReadSeriesTest.RegisterFlags(f)
	//cfg.Ct.Client.RegisterFlags(f)
	// cfg.TLSHealthTest.RegisterFlags(f)
	//cfg.Ct.RegisterFlags(f) // call embedded Config's RegisterFlags
	cfg.AlertingTest.RegisterFlags(f)
	cfg.TLSHealthTest.RegisterFlags(f)
	f.StringVar(&cfg.EnvConfigPath, "tests.env-config", "", "YAML file with environment definitions (endpoints + secrets manager info). If set, runs selected environments and exits.")
	f.StringVar(&cfg.EnvSelect, "tests.env-select", "all", "Which environments from the YAML to run: 'all' or a comma-separated list of names.")
	f.StringVar(&cfg.IAMEndpoint, "tests.iam-endpoint", "https://iam.cloud.ibm.com/identity/token", "IBM Cloud IAM token endpoint.")
	f.StringVar(&cfg.IBMAPIKeyEnv, "tests.ibm-apikey-env", "SERVICE_ID_API_KEY", "Environment variable name that contains the IBM Cloud API key.")

	// f.StringVar(&cfg.CACertFile, "tests.ca-cert-file", "", "Path to the CA certificate file for TLS")
	// f.StringVar(&cfg.ClientCertFile, "tests.client-cert-file", "", "Path to the client certificate file for TLS")
	// f.StringVar(&cfg.ClientKeyFile, "tests.client-key-file", "", "Path to the client key file for TLS")
	// f.StringVar(&cfg.QueryCertFile, "tests.query-cert-file", "", "Path to the query certificate file for TLS")
	// f.StringVar(&cfg.QueryKeyFile, "tests.query-key-file", "", "Path to the query key file for TLS")
}
