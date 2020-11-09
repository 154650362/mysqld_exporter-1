package master

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
	ini "gopkg.in/ini.v1"
)

//Instance message
type Instance struct {
	WorkGroupID   string `json:"workgroup_id"`   //mysql group id
	WorkGroupName string `json:"workgroup_name"` //workgroup name
	Addr          string `json:"instance_name"`  //ip port
	InstanceID    string `json:"instance_id"`    //Instance_id
	MysqlPort     string `json:"mysql_port"`     //mysql server port
	MysqlIP       string `json:"mysql_ip"`       //mysql server ip
	MysqlUser     string `json:"mysql_user"`     //默认root
	MysqlPassword string `json:"mysql_password"` //mysql password

}

var (
	tlsInsecureSkipVerify = kingpin.Flag(
		"tls.insecure-skip-verify",
		"Ignore certificate and server verification when using a tls connection.",
	).Bool()
	masterDSN string
)

//InstanceSlice 存放需上传的标签信息
var InstanceSlice []Instance

var count = 0

//GetInstance get dsn
func GetInstance() {
	Instance := &Instance{}
	//直接监控某个实例，不需要访问undb表
	//export SOURCE_DATA = ip1,port1,user1,pwd1,name1;ip2,port2,user2,pwd2,name2;ip3......
	teledbDsn := os.Getenv("SOURCE_DATA")
	if len(teledbDsn) != 0 {
		teledbs := strings.Split(teledbDsn, ":")
		for i := range teledbs {
			teledns := strings.Split(teledbs[i], ",")
			Instance.MysqlIP = teledns[0]
			Instance.MysqlPort = teledns[1]
			Instance.MysqlUser = teledns[2]
			Instance.MysqlPassword = teledns[3]
			Instance.WorkGroupID = " "
			Instance.WorkGroupName = teledns[4]
			Instance.InstanceID = " "
			Instance.Addr = Instance.MysqlIP + ":" + Instance.MysqlPort
			InstanceSlice = append(InstanceSlice, *Instance)
		}
		for i := 0; i < len(InstanceSlice); i++ {
			log.Println("InstanceSlice:", InstanceSlice[i])
		}
	} else {

		masterDSN = os.Getenv("DATA_SOURCE_NAME")
		if len(masterDSN) == 0 {
			//获取当前路径下的配置文件
			// dir, _ := os.Getwd()
			configMycnf := path.Join("/usr/local/services/mysql_exporter", "/exporter.cnf")
			log.Println("---------------" + configMycnf)
			var err error
			if masterDSN, err = parseMycnf(configMycnf); err != nil {
				// level.Info(logger).Log("msg", "Error parsing my.cnf", "file", *configMycnf, "err", err)
				log.Printf("Error parsing %s; err:%s", configMycnf, err.Error())
				os.Exit(1)
			}
		}
		db, err := sql.Open("mysql", masterDSN)
		log.Println("连接master数据库:" + masterDSN)
		if err != nil {
			log.Println("无法连接master数据库获取实例信息:" + err.Error())
		}
		defer db.Close()
		time.Sleep(5 * time.Second)

		for {
			rows := db.QueryRow("select count(1) as rows from (select i.ip_addr as host,i.port,i.password from instance i where i.instance_status = 'running') a;")
			if err != nil {
				log.Println("get instance message err: ", err)
				// continue
			}
			var rowstotal int
			err1 := rows.Scan(&rowstotal)
			if err1 != nil {
				log.Println("获取后行数失败" + err1.Error())
			}
			//判断是否有增加新的实例:两次查询结果对比是否有增加实例，如果有：清空数组，重新采集值，
			if count != rowstotal {
				//清空数组数据
				sliceClear(&InstanceSlice)
				//将查询到的最新的总实例数赋值给全局变量count
				count = rowstotal
				result, err := db.Query("select w.id as workgroup_id,w.`name` as workgroup_name,i.`name` as addr,i.id as intance_id,i.ip_addr as host,i.port,i.password from instance i,workgroup w where i.workgroup_id = w.id and i.instance_status = 'running'")

				for result.Next() {
					err = result.Scan(&Instance.WorkGroupID, &Instance.WorkGroupName, &Instance.Addr, &Instance.InstanceID, &Instance.MysqlIP, &Instance.MysqlPort, &Instance.MysqlPassword)
					if err != nil {
						log.Println(err.Error())
					}
					Instance.MysqlUser = "root"
					InstanceSlice = append(InstanceSlice, *Instance)
				}
			}
			for i := 0; i < len(InstanceSlice); i++ {
				log.Println("InstanceSlice:", InstanceSlice[i])
			}

			time.Sleep(1000 * time.Second)
		}
	}
}

//GetInstanceSlice test
func GetInstanceSlice() []Instance {
	return InstanceSlice
}

//清空数组
func sliceClear(s *[]Instance) {
	*s = append([]Instance{})
}

func parseMycnf(config interface{}) (string, error) {
	var dsn string
	opts := ini.LoadOptions{
		// MySQL ini file can have boolean keys.
		AllowBooleanKeys: true,
	}
	cfg, err := ini.LoadSources(opts, config)
	if err != nil {
		return dsn, fmt.Errorf("failed reading ini file: %s", err)
	}
	user := cfg.Section("client").Key("user").String()
	password := cfg.Section("client").Key("password").String()
	if (user == "") || (password == "") {
		return dsn, fmt.Errorf("no user or password specified under [client] in %s", config)
	}
	host := cfg.Section("client").Key("host").MustString("localhost")
	port := cfg.Section("client").Key("port").MustUint(3306)
	database := cfg.Section("client").Key("database").String()

	socket := cfg.Section("client").Key("socket").String()
	if socket != "" {
		dsn = fmt.Sprintf("%s:%s@unix(%s)/", user, password, socket)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, database)
	}
	sslCA := cfg.Section("client").Key("ssl-ca").String()
	sslCert := cfg.Section("client").Key("ssl-cert").String()
	sslKey := cfg.Section("client").Key("ssl-key").String()
	if sslCA != "" {
		if tlsErr := customizeTLS(sslCA, sslCert, sslKey); tlsErr != nil {
			tlsErr = fmt.Errorf("failed to register a custom TLS configuration for mysql dsn: %s", tlsErr)
			return dsn, tlsErr
		}
		dsn = fmt.Sprintf("%s?tls=custom", dsn)
	}

	return dsn, nil
}

func customizeTLS(sslCA string, sslCert string, sslKey string) error {
	var tlsCfg tls.Config
	caBundle := x509.NewCertPool()
	pemCA, err := ioutil.ReadFile(sslCA)
	if err != nil {
		return err
	}
	if ok := caBundle.AppendCertsFromPEM(pemCA); ok {
		tlsCfg.RootCAs = caBundle
	} else {
		return fmt.Errorf("failed parse pem-encoded CA certificates from %s", sslCA)
	}
	if sslCert != "" && sslKey != "" {
		certPairs := make([]tls.Certificate, 0, 1)
		keypair, err := tls.LoadX509KeyPair(sslCert, sslKey)
		if err != nil {
			return fmt.Errorf("failed to parse pem-encoded SSL cert %s or SSL key %s: %s",
				sslCert, sslKey, err)
		}
		certPairs = append(certPairs, keypair)
		tlsCfg.Certificates = certPairs
		tlsCfg.InsecureSkipVerify = *tlsInsecureSkipVerify
	}
	mysql.RegisterTLSConfig("custom", &tlsCfg)
	return nil
}
