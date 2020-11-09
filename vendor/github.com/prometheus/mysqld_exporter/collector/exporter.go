// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"ogit.un-net.com/triangle/mysqld_exporter/undb/master"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// Metric name parts.
const (
	// Subsystem(s).
	exporter = "exporter"
)

// SQL queries and parameters.
const (
	versionQuery = `SELECT @@version`

	// System variable params formatting.
	// See: https://github.com/go-sql-driver/mysql#system-variables
	sessionSettingsParam = `log_slow_filter=%27tmp_table_on_disk,filesort_on_disk%27`
	timeoutParam         = `lock_wait_timeout=%d`
)

var (
	versionRE = regexp.MustCompile(`^\d+\.\d+`)
)

// Tunable flags.
var (
	exporterLockTimeout = kingpin.Flag(
		"exporter.lock_wait_timeout",
		"Set a lock_wait_timeout on the connection to avoid long metadata locking.",
	).Default("2").Int()
	slowLogFilter = kingpin.Flag(
		"exporter.log_slow_filter",
		"Add a log_slow_filter to avoid slow query logging of scrapes. NOTE: Not supported by Oracle MySQL.",
	).Default("false").Bool()
)

// Metric descriptors.
var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, exporter, "collector_duration_seconds"),
		"Collector time duration.",
		[]string{"collector", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	upOrDownDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "up_or_down"),
		"Collector mysql up or down.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
)

// Verify if Exporter implements prometheus.Collector
var _ prometheus.Collector = (*Exporter)(nil)

// Exporter collects MySQL metrics. It implements prometheus.Collector.
type Exporter struct {
	ctx      context.Context
	logger   log.Logger
	dsn      string
	Instance master.Instance
	scrapers []Scraper
	metrics  Metrics
}

// New returns a new MySQL exporter for the provided DSN.
func New(ctx context.Context, dsn string, metrics Metrics, scrapers []Scraper, logger log.Logger, Instance master.Instance) *Exporter {
	// Setup extra params for the DSN, default to having a lock timeout.
	dsnParams := []string{fmt.Sprintf(timeoutParam, *exporterLockTimeout)}

	if *slowLogFilter {
		dsnParams = append(dsnParams, sessionSettingsParam)
	}

	if strings.Contains(dsn, "?") {
		dsn = dsn + "&"
	} else {
		dsn = dsn + "?"
	}
	dsn += strings.Join(dsnParams, "&")
	// level.Info(logger).Log("msg", "env address2", "address", dsn)
	return &Exporter{
		ctx:      ctx,
		logger:   logger,
		dsn:      dsn,
		Instance: Instance,
		scrapers: scrapers,
		metrics:  metrics,
	}
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.metrics.TotalScrapes.Desc()
	ch <- e.metrics.Error.Desc()
	e.metrics.ScrapeErrors.Describe(ch)
	ch <- e.metrics.MySQLUp.Desc()
}

// Collect implements prometheus.Collector.采集数据
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	//连接数据库采集数据
	e.scrape(e.ctx, ch, e.Instance)

	// ch <- e.metrics.TotalScrapes
	// ch <- e.metrics.Error
	// e.metrics.ScrapeErrors.Collect(ch)
	// ch <- e.metrics.MySQLUp
}

// func serverPing(target string) bool {
// 	pinger, err := ping.NewPinger(target)
// 	if err != nil {
// 		panic(err)
// 	}
// 	pinger.Count = 8
// 	pinger.Timeout = time.Duration(2 * time.Millisecond)
// 	pinger.SetPrivileged(true)
// 	pinger.Run() // blocks until finished
// 	stats := pinger.Statistics()
// 	fmt.Println(stats)
// 	// 有回包，就是说明IP是可用的
// 	if stats.PacketsRecv >= 1 {
// 		log.Println("success")
// 		return true
// 	}
// 	log.Println("failed")
// 	return false
// }

func (e *Exporter) scrape(ctx context.Context, ch chan<- prometheus.Metric, Instance master.Instance) {
	e.metrics.TotalScrapes.Inc()
	var err error

	scrapeTime := time.Now()
	// level.Info(e.logger).Log("msg", "begin to connect database to collect data", "dsn", e.dsn, "workgroup_name", e.Instance.WorkGroupName)
	db, err := sql.Open("mysql", e.dsn)
	if err != nil {
		level.Error(e.logger).Log("msg", "Error opening connection to database", "err", err)
		e.metrics.Error.Set(1)
		return
	}
	defer db.Close()

	// By design exporter should use maximum one connection per request.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// Set max lifetime for a connection.
	db.SetConnMaxLifetime(1 * time.Minute)

	// if !serverPing(e.Instance.MysqlIP) {
	// 	ch <- prometheus.MustNewConstMetric(upOrDownDesc, prometheus.GaugeValue, 2, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)

	// 	return
	// }

	if err := db.PingContext(ctx); err != nil {
		level.Error(e.logger).Log("msg", "Error pinging mysqld", "err", err)
		e.metrics.MySQLUp.Set(0)
		e.metrics.Error.Set(1)
		ch <- prometheus.MustNewConstMetric(upOrDownDesc, prometheus.GaugeValue, 0, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)
		return
	}
	ch <- prometheus.MustNewConstMetric(upOrDownDesc, prometheus.GaugeValue, 1, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)
	e.metrics.MySQLUp.Set(1)
	e.metrics.Error.Set(0)

	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, time.Since(scrapeTime).Seconds(), "connection", Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)

	version := getMySQLVersion(db)
	var wg sync.WaitGroup
	defer wg.Wait()
	//遍历了所有scraper，并go func调用所有scraper的Scrape()函数，实现对目标数据的采集
	for _, scraper := range e.scrapers {
		if version < scraper.Version() {
			continue
		}

		wg.Add(1)
		go func(scraper Scraper) {
			defer wg.Done()
			label := "collect." + scraper.Name()
			scrapeTime := time.Now()
			if err := scraper.Scrape(ctx, db, ch, log.With(e.logger, "scraper", scraper.Name()), Instance); err != nil {
				level.Error(e.logger).Log("msg", "Error from scraper", "scraper", scraper.Name(), "err", err)
				e.metrics.ScrapeErrors.WithLabelValues(label).Inc()
				e.metrics.Error.Set(1)
			}
			ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, time.Since(scrapeTime).Seconds(), label, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)
		}(scraper)
	}
}

func getMySQLVersion(db *sql.DB) float64 {
	var versionStr string
	var versionNum float64
	if err := db.QueryRow(versionQuery).Scan(&versionStr); err == nil {
		versionNum, _ = strconv.ParseFloat(versionRE.FindString(versionStr), 64)
	}
	// If we can't match/parse the version, set it some big value that matches all versions.
	if versionNum == 0 {
		versionNum = 999
	}
	return versionNum
}

// Metrics represents exporter metrics which values can be carried between http requests.
type Metrics struct {
	TotalScrapes prometheus.Counter
	ScrapeErrors *prometheus.CounterVec
	Error        prometheus.Gauge
	MySQLUp      prometheus.Gauge
}

// NewMetrics creates new Metrics instance.
func NewMetrics() Metrics {
	subsystem := exporter
	return Metrics{
		TotalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrapes_total",
			Help:      "Total number of times MySQL was scraped for metrics.",
		}),
		ScrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrape_errors_total",
			Help:      "Total number of times an error occurred scraping a MySQL.",
		}, []string{"collector"}),
		Error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from MySQL resulted in an error (1 for error, 0 for success).",
		}),
		MySQLUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the MySQL server is up.",
		}),
	}
}
