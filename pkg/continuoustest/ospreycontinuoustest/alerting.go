package ospreycontinuoustest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/continuoustest"
)

const metricName = "mimir_continuous_test_sine_wave_v2"

type AlertingTestConfig struct {
	Enabled                bool
	Namespace              string
	RuleGroupName          string
	AlertName              string
	AlertFor               time.Duration
	PollInterval           time.Duration
	PollTimeout            time.Duration
	AlertmanagerConfig     string
	AlertmanagerConfigFile string
	RuleGroupYAML          string
	RuleGroupFile          string
	ThresholdFactor        float64
}

func (cfg *AlertingTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.alerting-test.enabled", false, "Enable the alerting continuous test.")
	f.StringVar(&cfg.Namespace, "tests.alerting-test.namespace", "continuous-test", "Rules namespace used for the alerting test.")
	f.StringVar(&cfg.RuleGroupName, "tests.alerting-test.rule-group-name", "continuous-test", "Rule group name used for the alerting test.")
	f.StringVar(&cfg.AlertName, "tests.alerting-test.alert-name", "ContinuousTestSineHigh", "Alert name used in the default rule.")
	f.DurationVar(&cfg.AlertFor, "tests.alerting-test.alert-for", 0, "The 'for' duration for the default alert rule.")
	f.DurationVar(&cfg.PollInterval, "tests.alerting-test.poll-interval", 5*time.Second, "Interval between polls to the alerts API.")
	f.DurationVar(&cfg.PollTimeout, "tests.alerting-test.poll-timeout", 2*time.Minute, "Maximum time to wait for the alert to appear.")
	f.StringVar(&cfg.AlertmanagerConfig, "tests.alerting-test.alertmanager-config", "", "Inline Alertmanager YAML config to set for the test; if empty a dummy blackhole receiver is used.")
	f.StringVar(&cfg.AlertmanagerConfigFile, "tests.alerting-test.alertmanager-config-file", "", "Path to an Alertmanager YAML config file to set for the test.")
	f.StringVar(&cfg.RuleGroupYAML, "tests.alerting-test.rule-group-yaml", "", "Inline rule group YAML to upload; if empty a default metrics-based alert is generated.")
	f.StringVar(&cfg.RuleGroupFile, "tests.alerting-test.rule-group-file", "", "Path to a rule group YAML file to upload.")
	f.Float64Var(&cfg.ThresholdFactor, "tests.alerting-test.threshold-factor", 0.1, "Default alert threshold as a fraction of the number of series.")
}

type alertingTestMetrics struct {
	configPushesTotal       prometheus.Counter
	configPushesFailedTotal prometheus.Counter
	rulePushesTotal         prometheus.Counter
	rulePushesFailedTotal   prometheus.Counter
	alertChecksTotal        prometheus.Counter
	alertChecksFailedTotal  prometheus.Counter
}

func newAlertingTestMetrics(reg prometheus.Registerer) *alertingTestMetrics {
	return &alertingTestMetrics{
		configPushesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_alert_config_pushes_total",
			Help:        "Total number of attempted Alertmanager config pushes.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
		configPushesFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_alert_config_pushes_failed_total",
			Help:        "Total number of failed Alertmanager config pushes.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
		rulePushesTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_rule_group_pushes_total",
			Help:        "Total number of attempted rule group pushes.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
		rulePushesFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_rule_group_pushes_failed_total",
			Help:        "Total number of failed rule group pushes.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
		alertChecksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_alert_checks_total",
			Help:        "Total number of attempted alert list checks.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
		alertChecksFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "mimir_continuous_test_alert_checks_failed_total",
			Help:        "Total number of alert list checks that didn't find the expected alert.",
			ConstLabels: map[string]string{"test": "alerting"},
		}),
	}
}

type AlertingTest struct {
	name         string
	cfg          AlertingTestConfig
	client       *OspreyClient
	logger       log.Logger
	metrics      *alertingTestMetrics
	writeReadCfg continuoustest.WriteReadSeriesTestConfig
}

func NewAlertingTest(cfg AlertingTestConfig, writeReadCfg continuoustest.WriteReadSeriesTestConfig, clientCfg *OspreyClient, logger log.Logger, reg prometheus.Registerer) *AlertingTest {
	return &AlertingTest{
		name:         "alerting",
		cfg:          cfg,
		client:       clientCfg,
		logger:       log.With(logger, "test", "alerting"),
		metrics:      newAlertingTestMetrics(reg),
		writeReadCfg: writeReadCfg,
	}
}

func (t *AlertingTest) Name() string { return t.name }

func (t *AlertingTest) Init(ctx context.Context, now time.Time) error {
	amCfg := t.cfg.AlertmanagerConfig
	if amCfg == "" && t.cfg.AlertmanagerConfigFile != "" {
		b, err := os.ReadFile(t.cfg.AlertmanagerConfigFile)
		if err != nil {
			return fmt.Errorf("failed to read alertmanager config file: %w", err)
		}
		amCfg = string(b)
	}
	if amCfg == "" {
		amCfg = defaultAlertmanagerConfig()
	}

	if err := t.client.SetAlertmanagerConfig(ctx, amCfg, nil); err != nil {
		t.metrics.configPushesFailedTotal.Inc()
		level.Warn(t.logger).Log("msg", "Failed to push Alertmanager config", "err", err)
		return err
	}
	t.metrics.configPushesTotal.Inc()

	ruleYAML := t.cfg.RuleGroupYAML
	if ruleYAML == "" && t.cfg.RuleGroupFile != "" {
		b, err := os.ReadFile(t.cfg.RuleGroupFile)
		if err != nil {
			level.Debug(t.logger).Log("namespace", t.cfg.Namespace, "group", t.cfg.RuleGroupName)
			return fmt.Errorf("failed to read rule group file: %w", err)
		}
		ruleYAML = string(b)
	}
	if ruleYAML == "" {
		threshold := t.cfg.ThresholdFactor * float64(t.writeReadCfg.NumSeries)
		if threshold <= 0 {
			threshold = 1 // safeguard
		}
		ruleYAML = defaultRuleGroupYAML(t.cfg.RuleGroupName, t.cfg.AlertName, t.cfg.AlertFor, threshold)
	}

	level.Debug(t.logger).Log("msg", "Pushing rule group", "namespace", t.cfg.Namespace, "group_name", t.cfg.RuleGroupName)

	if err := t.client.CreateRuleGroup(ctx, t.cfg.Namespace, ruleYAML); err != nil {
		t.metrics.rulePushesFailedTotal.Inc()
		level.Warn(t.logger).Log("msg", "Failed to push rule group", "namespace", t.cfg.Namespace, "err", err)
		return err
	}
	t.metrics.rulePushesTotal.Inc()
	return nil
}

func (t *AlertingTest) Run(ctx context.Context, now time.Time) error {
	deadline := now.Add(t.cfg.PollTimeout)
	expectedAlert := t.cfg.AlertName

	for {
		if time.Now().After(deadline) {
			t.metrics.alertChecksFailedTotal.Inc()
			return fmt.Errorf("timeout waiting for alert %s to appear", expectedAlert)
		}

		t.metrics.alertChecksTotal.Inc()
		alerts, statusCode, err := t.client.ListActiveAlerts(ctx)
		if err != nil {
			level.Warn(t.logger).Log("msg", "Failed to list alerts", "status_code", statusCode, "err", err)
		} else {
			level.Debug(t.logger).Log("msg", "Active alerts snapshot", "count", len(alerts))

			for _, a := range alerts {
				level.Debug(t.logger).Log("msg", "Alert observed", "name", a.Name, "state", a.State, "labels", fmt.Sprintf("%v", a.Labels))
			}

			found := false
			for _, a := range alerts {
				if a.Name == expectedAlert && a.State == "firing" {
					found = true
					level.Debug(t.logger).Log("msg", "Found expected alert", "alert", expectedAlert)
					t.client.DeleteRuleNamespace(ctx, t.cfg.Namespace)
					return nil
				}
			}
			if !found {
				level.Debug(t.logger).Log("msg", "Expected alert not found in snapshot", "expected", expectedAlert)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.cfg.PollInterval):
		}
	}
}

func defaultAlertmanagerConfig() string {
	return "" +
		"route:\n" +
		"  receiver: devnull\n" +
		"receivers:\n" +
		"- name: devnull\n"
}

func defaultRuleGroupYAML(groupName, alertName string, alertFor time.Duration, threshold float64) string {
	forStr := "0s"
	if alertFor > 0 {
		forStr = alertFor.String()
	}

	expr := fmt.Sprintf("abs(sum(max_over_time(%s[30s]))) > %.3f", metricName, threshold)
	return fmt.Sprintf("name: %s\ninterval: 30s\nrules:\n  - alert: %s\n    expr: %s\n    for: %s\n    labels:\n      severity: test\n    annotations:\n      summary: 'Continuous test alert'\n", groupName, alertName, expr, forStr)
}
