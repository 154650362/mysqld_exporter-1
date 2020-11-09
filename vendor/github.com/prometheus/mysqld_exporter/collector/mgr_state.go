package collector

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"

	"ogit.un-net.com/triangle/mysqld_exporter/undb/master"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	//MGRSTATE mgr_state
	mgrState = "mgr_state_scraper"
)

/*
	对于字符串替换成对应数字上传说明：
	seconds_behinds_master:master->0;slave->实际值;mgr->-1
	slave_io/sql_running:yes->1;no->0
	single_mode:master->1;slave->0
	instance_mode：master/slave->0;mgr->1
	Mgr_Gtid_Reduction:master/slave->-1;mgr->实际值
	count_transactions_in_queue：master/slave->-1;mgr->实际值
	mgr_state:online->4;offline->3;other->2
	mgr_mode:primary->80;secondary->83
*/
var (
	secondsBehindMasterDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "seconds_behind_master"),
		"the slave second behind master value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	slaveSQLRunningDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "slave_sql_running"),
		"the slave slave SQL Running value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	slaveIORunningDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "slave_io_running"),
		"the slave slave IO Running value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	instanceModeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "instance_mode"),
		"the instance mode value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	singleModeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "single_mode"),
		"the instance single mode value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	mgrGtidReductionDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "mgr_gtid_reduction"),
		"the mgr gtid reduction value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	countTransactionsInQueueDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "count_transactions_in_queue"),
		"the count transactions in queue value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	mgrStateDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "mgr_state"),
		"the mgr state value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
	mgrModeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "mgr_mode"),
		"the mgr mode value.",
		[]string{"_workgroup_name", "workgroup_name", "addr", "cluster", "instanceId", "workgroup_id"}, nil,
	)
)

//ScrapeMgrState Collect data from mgr
type ScrapeMgrState struct{}

// Name of the Scraper. Should be unique.
func (ScrapeMgrState) Name() string {
	return mgrState
}

// Help describes the role of the Scraper.
func (ScrapeMgrState) Help() string {
	return "Collect from mgr_state"
}

// Version of MySQL from which scraper is available.
func (ScrapeMgrState) Version() float64 {
	return 5.7
}

//Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeMgrState) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger, Instance master.Instance) error {
	var tempsign string
	status := make(map[string]float64, 1000)
	sign, err := db.QueryContext(ctx, "select count(*) from performance_schema.replication_group_member_stats")
	if err != nil {
		level.Error(logger).Log("msg", "judge mgr or master-slave: ", "err", err)
	}
	count, _ := sign.Columns()
	temp := make([]sql.RawBytes, len(count))
	tempArgs := make([]interface{}, len(count))
	for i := range tempArgs {
		tempArgs[i] = &temp[i]
	}
	for sign.Next() {
		_ = sign.Scan(tempArgs...)
		for i, tempcol := range temp {
			if count[i] == "count(*)" {
				tempsign = string(tempcol)
			}
		}
	}

	//不为1表示为主从模式
	if tempsign != "1" {
		//取主从同步延迟信息
		result, err := db.QueryContext(ctx, "show slave status")
		if err != nil {
			level.Error(logger).Log("msg", "show slave status ", "err", err)
		}
		columns, err := result.Columns()
		if err != nil {
			level.Error(logger).Log("msg", "show slave status : ", "err", err)
		}
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		for result.Next() {
			//直接将result值写入scanArgs地址中（赋值给values）
			err = result.Scan(scanArgs...)
			if err != nil {
				level.Error(logger).Log("msg", "put result into scanArgs: ", "err", err)
			}
			for i, col := range values {
				if columns[i] == "Seconds_Behind_Master" {
					status[columns[i]], _ = strconv.ParseFloat(string(col), 64)
					ch <- prometheus.MustNewConstMetric(
						secondsBehindMasterDesc,
						prometheus.GaugeValue,
						status[columns[i]],
						Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
					)
				}
				if columns[i] == "Slave_IO_Running" {
					//值为yes，上传1；值为no,上传0；
					switch string(col) {
					case "Yes":
						ch <- prometheus.MustNewConstMetric(
							slaveIORunningDesc,
							prometheus.GaugeValue,
							1,
							Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
						)
					case "No":
						ch <- prometheus.MustNewConstMetric(
							slaveIORunningDesc,
							prometheus.GaugeValue,
							0,
							Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
						)
					}

				}
				if columns[i] == "Slave_SQL_Running" {
					//值为yes，上传1；值为no,上传0；
					switch string(col) {
					case "Yes":
						ch <- prometheus.MustNewConstMetric(
							slaveSQLRunningDesc,
							prometheus.GaugeValue,
							1,
							Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
						)
					case "No":
						ch <- prometheus.MustNewConstMetric(
							slaveSQLRunningDesc,
							prometheus.GaugeValue,
							0,
							Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
						)
					}
				}
			}

			// status["single_mode"] = "slave"
			//从节点指定single_mode为0
			ch <- prometheus.MustNewConstMetric(
				singleModeDesc,
				prometheus.GaugeValue,
				0,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
		}
		//不存在，则为主
		if _, ok := status["Seconds_Behind_Master"]; !ok {
			// status["Seconds_Behind_Master"] = "0"
			//主节点没有该值，设为0
			ch <- prometheus.MustNewConstMetric(
				secondsBehindMasterDesc,
				prometheus.GaugeValue,
				0,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
			// status["single_mode"] = "master"
			//主节点指定single_mode为1
			ch <- prometheus.MustNewConstMetric(
				singleModeDesc,
				prometheus.GaugeValue,
				1,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
		}
		// status["instance_mode"] = "single"
		//主从为0，mgr为1
		ch <- prometheus.MustNewConstMetric(
			instanceModeDesc,
			prometheus.GaugeValue,
			0,
			Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
		)
		// status["Mgr_Gtid_Reduction"] = "-1"
		// 主从模式为-1
		ch <- prometheus.MustNewConstMetric(
			mgrGtidReductionDesc,
			prometheus.GaugeValue,
			-1,
			Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
		)
		// status["COUNT_TRANSACTIONS_IN_QUEUE"] = "-1"
		//主从模式为-1
		ch <- prometheus.MustNewConstMetric(
			countTransactionsInQueueDesc,
			prometheus.GaugeValue,
			-1,
			Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
		)

	} else {
		//为1表示为mgr模式

		//获取mgr同步延迟信息
		//获取全局gtid
		{

			var globalTid string
			var sum int
			result, err := db.QueryContext(ctx, "select RECEIVED_TRANSACTION_SET from performance_schema.replication_connection_status where channel_name='group_replication_applier';")
			if err != nil {
				level.Error(logger).Log("msg", "获取全组GTID失败:", "err", err)
			}
			names, _ := result.Columns()
			values := make([]sql.RawBytes, len(names))
			Args := make([]interface{}, len(names))
			for i := range Args {
				Args[i] = &values[i]
			}

			for result.Next() {
				err = result.Scan(Args...)
				if err != nil {
					level.Error(logger).Log("msg", "获取全组GTID失败:", "err", err)
				}
				for _, col := range values {
					if len(col) == 0 {
						level.Info(logger).Log("msg", "正在获取全局GTID")
					} else {
						globalTid = string(col)
						//分析gtid是否唯一
						//不唯一的情况
						if strings.Contains(globalTid, ",") {
							globalTids := strings.Split(globalTid, ",")
							for _, val := range globalTids {
								res, err := getSingleGtidSum(val)
								if err != nil {
									level.Error(logger).Log("msg", "求globalgtid和失败:", "err", err)
								}
								sum += res
							}
							globalTid = strconv.Itoa(sum)
							sum = 0

						} else {
							//唯一的情况
							res, err := getSingleGtidSum(globalTid)
							if err != nil {
								level.Error(logger).Log("msg", "求globalgtid和失败:", "err", err)
							}
							globalTid = strconv.Itoa(res)
						}
					}
				}
			}

			var localTid string
			result, err = db.QueryContext(ctx, "select @@global.gtid_executed")
			if err != nil {
				level.Error(logger).Log("msg", "获取本地gtid失败:", "err", err)
			}
			names, _ = result.Columns()
			values = make([]sql.RawBytes, len(names))
			Args = make([]interface{}, len(names))
			for i := range Args {
				Args[i] = &values[i]
			}

			for result.Next() {
				err = result.Scan(Args...)
				if err != nil {
					level.Error(logger).Log("msg", "获取本地gtid失败:", "err", err, "time", time.Now())
					// continue
				}
				for _, col := range values {
					if len(col) == 0 {
						level.Info(logger).Log("msg", "正在获取本地GTID")
						// continue
					} else {
						localTid = string(col)
						//分析gtid是否唯一
						if strings.Contains(localTid, ",") {
							localTids := strings.Split(localTid, ",")
							for _, val := range localTids {
								res, err := getSingleGtidSum(val)
								if err != nil {
									level.Error(logger).Log("msg", "求localgtid和失败:", "err", err)
									// continue
								}
								sum += res

							}
							localTid = strconv.Itoa(sum)
							sum = 0
						} else {
							//唯一的情况
							res, err := getSingleGtidSum(localTid)
							if err != nil {
								level.Error(logger).Log("msg", "求localgtid和失败:", "err", err)
								// continue
							}
							localTid = strconv.Itoa(res)

						}

					}
				}
			}
			//得到组复制延迟
			global, _ := strconv.Atoi(globalTid)
			local, _ := strconv.Atoi(localTid)
			Reduction := global - local
			if Reduction < 0 {
				// status["Mgr_Gtid_Reduction"] = "-1"
				//本地gtid大于全局gtid,这种情况一般不存在
				ch <- prometheus.MustNewConstMetric(
					mgrGtidReductionDesc,
					prometheus.GaugeValue,
					-1,
					Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			} else {
				status["Mgr_Gtid_Reduction"] = float64(Reduction)
				ch <- prometheus.MustNewConstMetric(
					mgrGtidReductionDesc,
					prometheus.GaugeValue,
					status["Mgr_Gtid_Reduction"],
					Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
				)
			}
			// status["instance_mode"] = "mgr"
			//mgr模式为1
			ch <- prometheus.MustNewConstMetric(
				instanceModeDesc,
				prometheus.GaugeValue,
				1,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
			// status["Seconds_Behind_Master"] = "-1"
			ch <- prometheus.MustNewConstMetric(
				secondsBehindMasterDesc,
				prometheus.GaugeValue,
				-1,
				Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
			)
		}

		//查询mgr中待处理冲突检测对队列对事务数
		{
			result, err := db.QueryContext(ctx, "select COUNT_TRANSACTIONS_IN_QUEUE from performance_schema.replication_group_member_stats")
			if err != nil {
				level.Error(logger).Log("msg", "获取冲突检测事务数失败:", "err", err)
			}
			names, _ := result.Columns()
			values := make([]sql.RawBytes, len(names))
			Args := make([]interface{}, len(names))
			for i := range Args {
				Args[i] = &values[i]
			}
			for result.Next() {
				err = result.Scan(Args...)
				if err != nil {
					level.Error(logger).Log("msg", "获取冲突检测事务数失败:", "err", err, "time", time.Now())
				}
				for i, col := range values {
					if names[i] == "COUNT_TRANSACTIONS_IN_QUEUE" {
						status["COUNT_TRANSACTIONS_IN_QUEUE"], _ = strconv.ParseFloat(string(col), 64)
						ch <- prometheus.MustNewConstMetric(
							countTransactionsInQueueDesc,
							prometheus.GaugeValue,
							status["COUNT_TRANSACTIONS_IN_QUEUE"],
							Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
						)
					}
				}
			}
		}

		//查看mgr成员状态
		{
			result, err := db.QueryContext(ctx, "SELECT MEMBER_HOST,MEMBER_PORT,MEMBER_STATE,IF(global_status.VARIABLE_NAME IS NOT NULL,'PRIMARY','SECONDARY') AS MEMBER_ROLE FROM performance_schema.replication_group_members LEFT JOIN performance_schema.global_status ON global_status.VARIABLE_NAME = 'group_replication_primary_member' AND global_status.VARIABLE_VALUE = replication_group_members.MEMBER_ID where MEMBER_ID=@@server_uuid")
			if err != nil {
				level.Error(logger).Log("msg", "get mgr status err:", "err", err)
			}
			columns, err := result.Columns()
			if err != nil {
				level.Error(logger).Log("msg", "get mgr status err:", "err", err)
			}
			values := make([]sql.RawBytes, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}
			for result.Next() {
				err = result.Scan(scanArgs...)
				if err != nil {
					level.Error(logger).Log("msg", "put result into values:", "err", err)
				}
				for i, col := range values {
					if columns[i] == "MEMBER_STATE" {
						// status["mgr_state"] = string(col)
						switch string(col) {
						case "ONLINE":
							ch <- prometheus.MustNewConstMetric(
								mgrStateDesc,
								prometheus.GaugeValue,
								4,
								Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
							)
						case "OFFLINE":
							ch <- prometheus.MustNewConstMetric(
								mgrStateDesc,
								prometheus.GaugeValue,
								3,
								Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
							)
						default:
							ch <- prometheus.MustNewConstMetric(
								mgrStateDesc,
								prometheus.GaugeValue,
								2,
								Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
							)
						}

					}
					if columns[i] == "MEMBER_ROLE" {
						// status["mgr_mode"] = string(col)
						switch string(col) {
						case "PRIMARY":
							ch <- prometheus.MustNewConstMetric(
								mgrModeDesc,
								prometheus.GaugeValue,
								80,
								Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
							)
						case "SECONDARY":
							ch <- prometheus.MustNewConstMetric(
								mgrModeDesc,
								prometheus.GaugeValue,
								83,
								Instance.WorkGroupName, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupName, Instance.Addr, Instance.WorkGroupID,
							)
						}
						// //首先判断当前的角色为主
						// if status["mgr_mode"] == "PRIMARY" {
						// 	mgrMode = status["mgr_mode"]
						// }
						// //如果主不为空，则继续，这里为空，说明这个角色为从，跳过
						// if mgrMode != "" {
						// 	//将当前的角色传递给mgrmode1,
						// 	mgrMode1 = status["mgr_mode"]
						// 	//如果mgrmode=mgrmode1:角色一直都是主
						// 	//如果mgrmode!=mgrmode1:角色发生切换，主变为从
						// 	if mgrMode != mgrMode1 {
						// 		dirkillmessage := fmt.Sprintf(info.GetInstanceHomeDir() + "/kill_message.txt")
						// 		dirkill := fmt.Sprintf(info.GetInstanceHomeDir() + "/kill.txt")
						// 		log.Printf("%s change to %s need kill connected thread\n", mgrMode, mgrMode1)
						// 		sql1 := fmt.Sprintf("mysql %s -e \"select * from information_schema.processlist where DB != 'null';\" > %s", info.GetConnectOption(), dirkillmessage)
						// 		sql2 := fmt.Sprintf("mysql %s -e \"select concat('kill ',id,';') as t from information_schema.processlist where DB != 'null';\" | grep kill > %s", info.GetConnectOption(), dirkill)
						// 		sql3 := fmt.Sprintf("mysql %s -f < %s", info.GetConnectOption(), dirkill)
						// 		log.Println("killed message: " + dirkillmessage)
						// 		result1, err := utils.ExecCmd(sql1)
						// 		if err != nil {
						// 			log.Println("output thread_connected message  result: ", result1)
						// 		}
						// 		result2, err := utils.ExecCmd(sql2)
						// 		if err != nil {
						// 			log.Println("output thread_connected id  result: ", result2)
						// 		}
						// 		result3, err := utils.ExecCmd(sql3)
						// 		if err != nil {
						// 			log.Println("kill thread_connected id  result: ", result3)
						// 		}
						// 		//将mgrMode1赋值给mgrMode,做到只执行一次kill连接线程
						// 		mgrMode = mgrMode1

						// 	}
						// }
					}
				}
			}
		}
	}
	return nil
}

//求单一gtid的值
func getSingleGtidSum(s string) (int, error) {
	var suml = 0
	resArr := strings.Split(s, ":")
	resArr = resArr[1:len(resArr)]
	for _, vall := range resArr {
		if strings.Contains(vall, "-") {
			res, err := strconv.Atoi(strings.Split(vall, "-")[1])
			if err != nil {
				// level.Error(logger).Log("msg", "getSingleGtidSum error::", "err", err)
				return 0, err
			}
			suml += res
		} else {
			res, err := strconv.Atoi(vall)
			if err != nil {
				// level.Error(logger).Log("msg", "getSingleGtidSum error::", "err", err)
				return 0, err
			}
			suml += res
		}

	}
	return suml, nil
}

// check interface
var _ Scraper = ScrapeMgrState{}
