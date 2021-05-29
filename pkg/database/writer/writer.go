package writer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	//UpdateTemplate = `UPDATE "%s" SET %s WHERE "%s" = :primary_val`
	UpdateTemplate = "UPDATE `%s` SET %s WHERE `%s` = :primary_val"
	//InsertTemplate = `INSERT INTO "%s" (%s) VALUES (%s)`
	InsertTemplate = "INSERT INTO `%s` (%s) VALUES (%s)"
	//DeleteTemplate = `DELETE FROM "%s" WHERE "%s" = :primary_val`
	DeleteTemplate = "DELETE FROM `%s` WHERE `%s` = :primary_val"
)

type DatabaseInfo struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Secure   bool   `json:"secure"`
	Username string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

type RecordDef struct {
	HasPrimary    bool
	PrimaryColumn string
	Values        map[string]interface{}
	ColumnDefs    []*ColumnDef
}

type ColumnDef struct {
	ColumnName  string
	BindingName string
	Value       interface{}
}

type DBCommand struct {
	QueryStr string
	Args     map[string]interface{}
}

type Writer struct {
	dbInfo   *DatabaseInfo
	db       *sqlx.DB
	commands chan *DBCommand
}

func NewWriter() *Writer {
	return &Writer{
		dbInfo:   &DatabaseInfo{},
		commands: make(chan *DBCommand, 2048),
	}
}

func (writer *Writer) Init() error {

	// Read configuration file
	writer.dbInfo.Host = viper.GetString("database.host")
	writer.dbInfo.Port = viper.GetInt("database.port")
	writer.dbInfo.Secure = viper.GetBool("database.secure")
	writer.dbInfo.Username = viper.GetString("database.username")
	writer.dbInfo.Password = viper.GetString("database.password")
	writer.dbInfo.DbName = viper.GetString("database.dbname")

	log.WithFields(log.Fields{
		"host":     writer.dbInfo.Host,
		"port":     writer.dbInfo.Port,
		"secure":   writer.dbInfo.Secure,
		"username": writer.dbInfo.Username,
		"dbname":   writer.dbInfo.DbName,
	}).Info("Connecting to database")

	var params map[string]string
	if writer.dbInfo.Secure {
		params["tls"] = "true"
	}

	config := mysql.Config{
		User:                 writer.dbInfo.Username,
		Passwd:               writer.dbInfo.Password,
		Addr:                 fmt.Sprintf("%s:%d", writer.dbInfo.Host, writer.dbInfo.Port),
		Net:                  "tcp",
		DBName:               writer.dbInfo.DbName,
		AllowNativePasswords: true,
		Params:               params,
	}

	connStr := config.FormatDSN()

	// Open database
	db, err := sqlx.Open("mysql", connStr)
	if err != nil {
		log.Error(err)
		return err
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	writer.db = db

	go writer.run()

	return nil
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			_, err := writer.db.NamedExec(cmd.QueryStr, cmd.Args)
			if err != nil {
				log.Error(err)
			}
		}
	}
}

func (writer *Writer) ProcessData(record *gravity_sdk_types_record.Record) error {

	log.WithFields(log.Fields{
		"method": record.Method,
		"event":  record.EventName,
		"table":  record.Table,
	}).Info("Write record")

	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(record)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(record)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(record)
	}

	return nil
}

func (writer *Writer) GetDefinition(record *gravity_sdk_types_record.Record) (*RecordDef, error) {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := gravity_sdk_types_record.GetValue(field.Value)

		// Primary key
		//if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {
			recordDef.Values["primary_val"] = value
			recordDef.HasPrimary = true
			recordDef.PrimaryColumn = field.Name
			continue
		}

		// Generate binding name
		bindingName := fmt.Sprintf("val_%s", strconv.Itoa(n))
		recordDef.Values[bindingName] = value

		// Store definition
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &ColumnDef{
			ColumnName:  field.Name,
			Value:       field.Name,
			BindingName: bindingName,
		})
	}

	if len(record.PrimaryKey) > 0 && !recordDef.HasPrimary {
		log.WithFields(log.Fields{
			"column": record.PrimaryKey,
		}).Error("Not found primary key")

		return nil, errors.New("Not found primary key")
	}

	return recordDef, nil
}

func (writer *Writer) InsertRecord(record *gravity_sdk_types_record.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	return writer.insert(record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(record *gravity_sdk_types_record.Record) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err = writer.update(record.Table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(record *gravity_sdk_types_record.Record) error {

	if record.PrimaryKey == "" {
		// Do nothing
		return nil
	}

	for _, field := range record.Fields {

		// Primary key
		//if field.IsPrimary == true {
		if record.PrimaryKey == field.Name {

			value := gravity_sdk_types_record.GetValue(field.Value)

			sqlStr := fmt.Sprintf(DeleteTemplate, record.Table, field.Name)

			writer.commands <- &DBCommand{
				QueryStr: sqlStr,
				Args: map[string]interface{}{
					"primary_val": value,
				},
			}

			break
		}
	}

	return nil
}

func (writer *Writer) update(table string, recordDef *RecordDef) (bool, error) {

	// Preparing SQL string
	updates := make([]string, 0, len(recordDef.ColumnDefs))
	for _, def := range recordDef.ColumnDefs {
		updates = append(updates, "`"+def.ColumnName+"` = :"+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(UpdateTemplate, table, updateStr, recordDef.PrimaryColumn)

	writer.commands <- &DBCommand{
		QueryStr: sqlStr,
		Args:     recordDef.Values,
	}

	return false, nil
}

func (writer *Writer) insert(table string, recordDef *RecordDef) error {

	paramLength := len(recordDef.ColumnDefs)
	if recordDef.HasPrimary {
		paramLength++
	}

	// Allocation
	colNames := make([]string, 0, paramLength)
	valNames := make([]string, 0, paramLength)

	if recordDef.HasPrimary {
		colNames = append(colNames, "`"+recordDef.PrimaryColumn+"`")
		valNames = append(valNames, ":primary_val")
	}

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, "`"+def.ColumnName+"`")
		valNames = append(valNames, `:`+def.BindingName)
	}

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(InsertTemplate, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	writer.commands <- &DBCommand{
		QueryStr: insertStr,
		Args:     recordDef.Values,
	}

	return nil
}
