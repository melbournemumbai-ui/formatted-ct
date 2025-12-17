package continuoustest

import (
	"flag"

	"github.com/grafana/dskit/flagext"
)

type Config struct {
	Client              ClientConfig              `yaml:"-"`
	Manager             ManagerConfig             `yaml:"-"`
	WriteReadSeriesTest WriteReadSeriesTestConfig `yaml:"-"`
	EnvConfigPath       string                    `yaml:"-"`
	EnvSelect           string                    `yaml:"-"`
	IAMEndpoint         string                    `yaml:"-"`
	IBMAPIKeyEnv        string                    `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
	f.StringVar(&cfg.EnvConfigPath, "tests.env-config", "", "YAML file with environment definitions (endpoints + secrets manager info). If set, runs selected environments and exits.")
	f.StringVar(&cfg.EnvSelect, "tests.env-select", "all", "Which environments from the YAML to run: 'all' or a comma-separated list of names.")
	f.StringVar(&cfg.IAMEndpoint, "tests.iam-endpoint", "https://iam.cloud.ibm.com/identity/token", "IBM Cloud IAM token endpoint.")
	f.StringVar(&cfg.IBMAPIKeyEnv, "tests.ibm-apikey-env", "SERVICE_ID_API_KEY", "Environment variable name that contains the IBM Cloud API key.")
}

var _ flagext.Registerer = (*Config)(nil)
