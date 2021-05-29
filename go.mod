module github.com/BrobridgeOrg/gravity-transmitter-mysql

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.2.14
	github.com/BrobridgeOrg/gravity-sdk v0.0.12
	github.com/go-sql-driver/mysql v1.5.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.8.0
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
)

//replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
