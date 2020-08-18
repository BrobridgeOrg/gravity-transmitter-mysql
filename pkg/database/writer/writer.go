package writer

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	transmitter "github.com/BrobridgeOrg/gravity-api/service/transmitter"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
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

func (writer *Writer) ProcessData(record *transmitter.Record) error {

	switch record.Method {
	case transmitter.Method_DELETE:
		return writer.DeleteRecord(record)
	case transmitter.Method_UPDATE:
		return writer.UpdateRecord(record)
	case transmitter.Method_INSERT:
	}

	return nil
}

func (writer *Writer) GetValue(value *transmitter.Value) interface{} {

	switch value.Type {
	case transmitter.DataType_FLOAT64:
		return float64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_INT64:
		return int64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_UINT64:
		return uint64(binary.LittleEndian.Uint64(value.Value))
	case transmitter.DataType_BOOLEAN:
		return int8(value.Value[0]) & 1
	case transmitter.DataType_STRING:
		return string(value.Value)
	}

	// binary
	return value.Value
}

func (writer *Writer) GetDefinition(record *transmitter.Record) *RecordDef {

	recordDef := &RecordDef{
		HasPrimary: false,
		Values:     make(map[string]interface{}),
		ColumnDefs: make([]*ColumnDef, 0, len(record.Fields)),
	}

	// Scanning fields
	for n, field := range record.Fields {

		value := writer.GetValue(field.Value)

		// Primary key
		if field.IsPrimary == true {
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

	return recordDef
}

func (writer *Writer) InsertRecord(record *transmitter.Record) error {

	recordDef := writer.GetDefinition(record)

	return writer.insert(record.Table, recordDef)
}

func (writer *Writer) UpdateRecord(record *transmitter.Record) error {

	recordDef := writer.GetDefinition(record)

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	// TODO: performance issue because do twice for each record
	err := writer.insert(record.Table, recordDef)
	if err != nil {
		return err
	}

	_, err = writer.update(record.Table, recordDef)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(record *transmitter.Record) error {

	template := "DELETE FROM `%s` WHERE `%s` = :primary_val"

	for _, field := range record.Fields {

		// Primary key
		if field.IsPrimary == true {

			value := writer.GetValue(field.Value)

			sqlStr := fmt.Sprintf(template, record.Table, field.Name)

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
	template := "UPDATE `%s` SET %s WHERE `%s` = :primary_val"
	for _, def := range recordDef.ColumnDefs {
		updates = append(updates, "`"+def.ColumnName+"` = :"+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(template, table, updateStr, recordDef.PrimaryColumn)

	writer.commands <- &DBCommand{
		QueryStr: sqlStr,
		Args:     recordDef.Values,
	}

	return false, nil
}

func (writer *Writer) insert(table string, recordDef *RecordDef) error {

	// Insert a new record
	colNames := []string{
		recordDef.PrimaryColumn,
	}
	valNames := []string{
		":primary_val",
	}

	// Preparing columns and bindings
	for _, def := range recordDef.ColumnDefs {
		colNames = append(colNames, "`"+def.ColumnName+"`")
		valNames = append(valNames, `:`+def.BindingName)
	}

	template := "INSERT INTO `%s` (%s) VALUES (%s)"

	// Preparing SQL string to insert
	colsStr := strings.Join(colNames, ",")
	valsStr := strings.Join(valNames, ",")
	insertStr := fmt.Sprintf(template, table, colsStr, valsStr)

	//	database.db.NamedExec(insertStr, recordDef.Values)
	writer.commands <- &DBCommand{
		QueryStr: insertStr,
		Args:     recordDef.Values,
	}

	return nil
}
