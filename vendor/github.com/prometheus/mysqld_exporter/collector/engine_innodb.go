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

// Scrape `SHOW ENGINE INNODB STATUS`.

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
	// Subsystem.
	innodb = "engine_innodb"
	// Query.
	engineInnodbStatusQuery = `SHOW ENGINE INNODB STATUS`
)

// ScrapeEngineInnodbStatus scrapes from `SHOW ENGINE INNODB STATUS`.
type ScrapeEngineInnodbStatus struct{}

// Name of the Scraper. Should be unique.
func (ScrapeEngineInnodbStatus) Name() string {
	return "engine_innodb_status"
}

// Help describes the role of the Scraper.
func (ScrapeEngineInnodbStatus) Help() string {
	return "Collect from SHOW ENGINE INNODB STATUS"
}

// Version of MySQL from which scraper is available.
func (ScrapeEngineInnodbStatus) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeEngineInnodbStatus) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger, Instance master.Instance) error {
	rows, err := db.QueryContext(ctx, engineInnodbStatusQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var typeCol, nameCol, statusCol string
	// First row should contain the necessary info. If many rows returned then it's unknown case.
	if rows.Next() {
		if err := rows.Scan(&typeCol, &nameCol, &statusCol); err != nil {
			return err
		}
	}

	// 0 queries inside InnoDB, 0 queries in queue
	// 0 read views open inside InnoDB
	rQueries, _ := regexp.Compile(`(\d+) queries inside InnoDB, (\d+) queries in queue`)
	rViews, _ := regexp.Compile(`(\d+) read views open inside InnoDB`)

	for _, line := range strings.Split(statusCol, "\n") {
		if data := rQueries.FindStringSubmatch(line); data != nil {
			value, _ := strconv.ParseFloat(data[1], 64)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(prometheus.BuildFQName(innodb, "queries_inside_innodb", "Queries_inside_InnoDB"),
					"Queries inside InnoDB",
					[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil),
				prometheus.GaugeValue,
				value,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
			value, _ = strconv.ParseFloat(data[2], 64)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(prometheus.BuildFQName(innodb, "queries_in_queue", "queries_in_queue"),
					"queries in queue",
					[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil),
				prometheus.GaugeValue,
				value,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)

		} else if data := rViews.FindStringSubmatch(line); data != nil {
			value, _ := strconv.ParseFloat(data[1], 64)
			ch <- prometheus.MustNewConstMetric(
				prometheus.NewDesc(prometheus.BuildFQName(innodb, "read_views_open_inside_innodb", "read_views_open_inside_innodb"),
					"read views open inside innodb",
					[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil),
				prometheus.GaugeValue,
				value,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeEngineInnodbStatus{}
