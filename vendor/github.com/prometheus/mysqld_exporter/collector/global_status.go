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

// Scrape `SHOW GLOBAL STATUS`.

package collector

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"

	"ogit.un-net.com/triangle/mysqld_exporter/undb/master"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// Scrape query.
	globalStatusQuery = `SHOW GLOBAL STATUS`
	// Subsystem.
	globalStatus = "global_status"
)

// Regexp to match various groups of status vars.
var globalStatusRE = regexp.MustCompile(`^(com|handler|connection_errors|innodb_buffer_pool_pages|innodb_rows|performance_schema)_(.*)$`)

// Metric descriptors.
var (
// globalCommandsDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "commands_total"),
// 	"Total number of executed MySQL commands.",
// 	[]string{"command", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalHandlerDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "handlers_total"),
// 	"Total number of executed MySQL handlers.",
// 	[]string{"handler", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalConnectionErrorsDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "connection_errors_total"),
// 	"Total number of MySQL connection errors.",
// 	[]string{"error", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalBufferPoolPagesDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "buffer_pool_pages"),
// 	"Innodb buffer pool pages by state.",
// 	[]string{"state", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalBufferPoolDirtyPagesDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "buffer_pool_dirty_pages"),
// 	"Innodb buffer pool dirty pages.",
// 	[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalBufferPoolPageChangesDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "buffer_pool_page_changes_total"),
// 	"Innodb buffer pool page state changes.",
// 	[]string{"operation", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalInnoDBRowOpsDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "innodb_row_ops_total"),
// 	"Total number of MySQL InnoDB row operations.",
// 	[]string{"operation", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
// globalPerformanceSchemaLostDesc = prometheus.NewDesc(
// 	prometheus.BuildFQName(namespace, globalStatus, "performance_schema_lost_total"),
// 	"Total number of MySQL instrumentations that could not be loaded or created due to memory constraints.",
// 	[]string{"instrumentation", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
// )
)

// ScrapeGlobalStatus collects from `SHOW GLOBAL STATUS`.
type ScrapeGlobalStatus struct{}

// Name of the Scraper. Should be unique.
func (ScrapeGlobalStatus) Name() string {
	return globalStatus
}

// Help describes the role of the Scraper.
func (ScrapeGlobalStatus) Help() string {
	return "Collect from SHOW GLOBAL STATUS"
}

// Version of MySQL from which scraper is available.
func (ScrapeGlobalStatus) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeGlobalStatus) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger, Instance master.Instance) error {

	globalStatusRows, err := db.QueryContext(ctx, globalStatusQuery)
	if err != nil {
		return err
	}
	defer globalStatusRows.Close()

	var key string
	var val sql.RawBytes
	var textItems = map[string]string{
		"wsrep_local_state_uuid":   "",
		"wsrep_cluster_state_uuid": "",
		"wsrep_provider_version":   "",
		"wsrep_evs_repl_latency":   "",
	}

	for globalStatusRows.Next() {
		if err := globalStatusRows.Scan(&key, &val); err != nil {
			return err
		}
		if floatVal, ok := parseStatus(val); ok { // Unparsable values are silently skipped.
			key = validPrometheusName(key)
			match := globalStatusRE.FindStringSubmatch(key)
			if match == nil {
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(prometheus.BuildFQName(namespace, "", key),
						"Generic metric from SHOW GLOBAL STATUS.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.UntypedValue,
					floatVal,
					Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
				continue
			}
			switch match[1] {
			case "com":
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, "com", match[2]),
						"Total number of executed MySQL commands.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			case "handler":
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, "handler", match[2]),
						"Total number of executed MySQL handlers.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			case "connection_errors":
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, "connection_errors", match[2]),
						"Total number of MySQL connection errors.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			case "innodb_buffer_pool_pages":
				switch match[2] {
				case "data", "free", "misc", "old":
					ch <- prometheus.MustNewConstMetric(
						prometheus.NewDesc(
							prometheus.BuildFQName(namespace, "innodb_buffer_pool_pages", match[2]),
							"Innodb buffer pool pages by state.",
							[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
						),
						prometheus.GaugeValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
					)
				case "dirty":
					ch <- prometheus.MustNewConstMetric(
						prometheus.NewDesc(
							prometheus.BuildFQName(namespace, "innodb_buffer_pool_pages", "dirty"),
							"Innodb buffer pool dirty pages.",
							[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
						),
						prometheus.GaugeValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
					)
				case "total":
					// continue
					ch <- prometheus.MustNewConstMetric(
						prometheus.NewDesc(
							prometheus.BuildFQName(namespace, "innodb_buffer_pool_pages", match[2]),
							"Innodb buffer pool page state total.",
							[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
						),
						prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
					)
				default:
					ch <- prometheus.MustNewConstMetric(
						prometheus.NewDesc(
							prometheus.BuildFQName(namespace, "innodb_buffer_pool_pages", match[2]),
							"Innodb buffer pool page state changes.",
							[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
						),
						prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
					)
				}
			case "innodb_rows":
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, "innodb_rows", match[2]),
						"Total number of MySQL InnoDB row operations.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			case "performance_schema":
				ch <- prometheus.MustNewConstMetric(
					prometheus.NewDesc(
						prometheus.BuildFQName(namespace, "performance_schema", match[2]),
						"Total number of MySQL instrumentations that could not be loaded or created due to memory constraints.",
						[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
					),
					prometheus.CounterValue, floatVal, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			}
		} else if _, ok := textItems[key]; ok {
			textItems[key] = string(val)
		}
	}

	// mysql_galera_variables_info metric.
	if textItems["wsrep_local_state_uuid"] != "" {
		ch <- prometheus.MustNewConstMetric(
			prometheus.NewDesc(prometheus.BuildFQName(namespace, "galera", "status_info"), "PXC/Galera status information.",
				[]string{"wsrep_local_state_uuid", "wsrep_cluster_state_uuid", "wsrep_provider_version", "_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil),
			prometheus.GaugeValue, 1, textItems["wsrep_local_state_uuid"], textItems["wsrep_cluster_state_uuid"], textItems["wsrep_provider_version"], Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
		)
	}

	// mysql_galera_evs_repl_latency
	if textItems["wsrep_evs_repl_latency"] != "" {

		type evsValue struct {
			name  string
			value float64
			index int
			help  string
		}

		evsMap := []evsValue{
			evsValue{name: "min_seconds", value: 0, index: 0, help: "PXC/Galera group communication latency. Min value."},
			evsValue{name: "avg_seconds", value: 0, index: 1, help: "PXC/Galera group communication latency. Avg value."},
			evsValue{name: "max_seconds", value: 0, index: 2, help: "PXC/Galera group communication latency. Max value."},
			evsValue{name: "stdev", value: 0, index: 3, help: "PXC/Galera group communication latency. Standard Deviation."},
			evsValue{name: "sample_size", value: 0, index: 4, help: "PXC/Galera group communication latency. Sample Size."},
		}

		evsParsingSuccess := true
		values := strings.Split(textItems["wsrep_evs_repl_latency"], "/")

		if len(evsMap) == len(values) {
			for i, v := range evsMap {
				evsMap[i].value, err = strconv.ParseFloat(values[v.index], 64)
				if err != nil {
					evsParsingSuccess = false
				}
			}

			if evsParsingSuccess {
				for _, v := range evsMap {
					key := prometheus.BuildFQName(namespace, "galera_evs_repl_latency", v.name)
					desc := prometheus.NewDesc(key, v.help, []string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil)
					ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v.value, Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID)
				}
			}
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeGlobalStatus{}
