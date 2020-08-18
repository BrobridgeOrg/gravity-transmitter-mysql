module github.com/BrobridgeOrg/gravity-transmitter-mysql

go 1.13

require (
	github.com/BrobridgeOrg/gravity-api v0.0.0-20200810192326-098209cf878f
	github.com/BrobridgeOrg/gravity-transmitter-postgres v0.0.0-20200813201447-fd683654e2a7
	github.com/go-sql-driver/mysql v1.5.0
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.8.0
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/viper v1.7.1
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	google.golang.org/grpc v1.31.0
)

replace github.com/BrobridgeOrg/gravity-api => ../gravity-api
