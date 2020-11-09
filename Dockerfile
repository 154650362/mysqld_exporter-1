# FROM alpine:latest
# ADD bin/mysqlexporter /usr/local/bin/mysqlexporter
# ADD conf/exporter.cnf /usr/local/services/mysql_exporter/exporter.cnf
# EXPOSE 8080
# ENTRYPOINT ["/usr/local/bin/mysqlexporter"]

FROM alpine:latest
RUN mkdir -p /usr/local/services/mysql_exporter
WORKDIR /usr/local/services/mysql_exporter
ADD bin/mysqlexporter /usr/local/services/mysql_exporter
ADD conf/exporter.cnf /usr/local/services/mysql_exporter
RUN chmod +x /usr/local/services/mysql_exporter/mysqlexporter
EXPOSE 8080
CMD [ "/usr/local/services/mysql_exporter/mysqlexporter" ]