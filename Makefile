GOOS=$(shell go env | grep GOOS | awk -F "=" '{print $$NF}' | awk -F "\"" '{print $$2}')
GOARCH=$(shell go env | grep GOARCH | awk -F "=" '{print $$NF}' | awk -F "\"" '{print $$2}')
SERVICE=$(shell pwd | awk -F "/" '{print $$NF}')
BINARY=$(SERVICE)-$(GOOS)-$(GOARCH)
TAG?=latest

default: build
.PHONY: build
build:
	GOOS=$(GOOS) GOARCH=$(GOARCH) go build -tags netgo -o ./bin/mysqlexporter ./mysqld_exporter.go
limage: linux
	docker build -t registry.un-net.com/triangle/undb-with-exporter:$(TAG) .	
lpush: linux
	docker push registry.un-net.com/triangle/undb-with-exporter:$(TAG)	
find:
	ps aux | grep "./bin/mysqlexporter" | grep -v "grep"
watch:
	echo 'ps aux | grep "./bin/mysqlexporter" | grep -v "grep" | awk '{print $$2}' | xargs top -d 0.5 -p' 
clean:
	rm -rf ./bin/mysqlexporter
	docker rmi registry.un-net.com/triangle/undb-with-mysqlexporter:$(TAG)
