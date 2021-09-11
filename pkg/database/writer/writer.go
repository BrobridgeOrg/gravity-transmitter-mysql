package writer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	gravity_sdk_types_record "github.com/BrobridgeOrg/gravity-sdk/types/record"
	"github.com/BrobridgeOrg/gravity-transmitter-mysql/pkg/database"
	buffered_input "github.com/cfsghost/buffered-input"
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

var recordDefPool = sync.Pool{
	New: func() interface{} {
		return &gravity_sdk_types_record.RecordDef{}
	},
}

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

type Writer struct {
	dbInfo            *DatabaseInfo
	db                *sqlx.DB
	commands          chan *DBCommand
	completionHandler database.CompletionHandler
	buffer            *buffered_input.BufferedInput
}

func NewWriter() *Writer {
	writer := &Writer{
		dbInfo:            &DatabaseInfo{},
		commands:          make(chan *DBCommand, 2048),
		completionHandler: func(database.DBCommand) {},
	}

	// Initializing buffered input
	opts := buffered_input.NewOptions()
	opts.ChunkSize = viper.GetInt("bufferInput.chunkSize")
	opts.ChunkCount = 10000
	opts.Timeout = viper.GetDuration("bufferInput.timeout") * time.Millisecond
	opts.Handler = writer.chunkHandler
	writer.buffer = buffered_input.NewBufferedInput(opts)

	return writer
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

func (writer *Writer) chunkHandler(chunk []interface{}) {

	dbCommands := make([]*DBCommand, 0, len(chunk))
	for _, request := range chunk {
		req := request.(*DBCommand)
		dbCommands = append(dbCommands, req)
	}
	writer.processData(dbCommands)
}

func (writer *Writer) processData(dbCommands []*DBCommand) {
	// Write to Database
	for {
	LOOP:
		tx, err := writer.db.Beginx()
		if err != nil {
			log.Error(err)
			tx.Rollback()

			<-time.After(time.Second * 5)

			log.WithFields(log.Fields{}).Warn("Retry to write record to database by batch ...")
			continue
		}

		for _, cmd := range dbCommands {
			_, err := tx.NamedExec(cmd.QueryStr, cmd.Args)
			if err != nil {
				log.WithFields(log.Fields{
					"pkey_field": cmd.Record.PrimaryKey,
				}).Error(err)
				log.Error(cmd.QueryStr)
				log.Error(cmd.Args)
				tx.Rollback()
				<-time.After(time.Second * 5)
				goto LOOP

			}
		}
		err = tx.Commit()

		if err != nil {
			log.Error(err)
			tx.Rollback()

			<-time.After(time.Second * 5)

			log.WithFields(log.Fields{}).Warn("Retry to write record to database by batch ...")
			continue
		}

		break
	}

	for _, cmd := range dbCommands {
		writer.completionHandler(database.DBCommand(cmd))
		recordDefPool.Put(cmd.RecordDef)
		dbCommandPool.Put(cmd)
	}
}

func (writer *Writer) run() {
	for {
		select {
		case cmd := <-writer.commands:
			// publish to buffered-input
			writer.buffer.Push(cmd)
		}
	}
}

func (writer *Writer) SetCompletionHandler(fn database.CompletionHandler) {
	writer.completionHandler = fn
}

func (writer *Writer) ProcessData(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	switch record.Method {
	case gravity_sdk_types_record.Method_DELETE:
		return writer.DeleteRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_UPDATE:
		return writer.UpdateRecord(reference, record, tables)
	case gravity_sdk_types_record.Method_INSERT:
		return writer.InsertRecord(reference, record, tables)
	}

	return nil
}

func (writer *Writer) GetDefinition(record *gravity_sdk_types_record.Record) (*gravity_sdk_types_record.RecordDef, error) {

	recordDef := recordDefPool.Get().(*gravity_sdk_types_record.RecordDef)
	recordDef.HasPrimary = false
	recordDef.Values = make(map[string]interface{})
	recordDef.ColumnDefs = make([]*gravity_sdk_types_record.ColumnDef, 0, len(record.Fields))

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
		recordDef.ColumnDefs = append(recordDef.ColumnDefs, &gravity_sdk_types_record.ColumnDef{
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

func (writer *Writer) InsertRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	return writer.insert(reference, record, record.Table, recordDef, tables)
}

func (writer *Writer) UpdateRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

	recordDef, err := writer.GetDefinition(record)
	if err != nil {
		return err
	}

	// Ignore if no primary key
	if recordDef.HasPrimary == false {
		return nil
	}

	_, err = writer.update(reference, record, record.Table, recordDef, tables)
	if err != nil {
		return err
	}

	return nil
}

func (writer *Writer) DeleteRecord(reference interface{}, record *gravity_sdk_types_record.Record, tables []string) error {

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

			dbCommand := dbCommandPool.Get().(*DBCommand)
			dbCommand.Reference = reference
			dbCommand.Record = record
			dbCommand.QueryStr = sqlStr
			dbCommand.Args = map[string]interface{}{
				"primary_val": value,
			}
			dbCommand.Tables = tables

			writer.commands <- dbCommand

			break
		}
	}

	return nil
}

func (writer *Writer) update(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef, tables []string) (bool, error) {

	// Preparing SQL string
	updates := make([]string, 0, len(recordDef.ColumnDefs))
	for _, def := range recordDef.ColumnDefs {
		updates = append(updates, "`"+def.ColumnName+"` = :"+def.BindingName)
	}

	updateStr := strings.Join(updates, ",")
	sqlStr := fmt.Sprintf(UpdateTemplate, table, updateStr, recordDef.PrimaryColumn)

	dbCommand := dbCommandPool.Get().(*DBCommand)
	dbCommand.Reference = reference
	dbCommand.Record = record
	dbCommand.QueryStr = sqlStr
	dbCommand.Args = recordDef.Values
	dbCommand.RecordDef = recordDef
	dbCommand.Tables = tables

	writer.commands <- dbCommand

	return false, nil
}

func (writer *Writer) insert(reference interface{}, record *gravity_sdk_types_record.Record, table string, recordDef *gravity_sdk_types_record.RecordDef, tables []string) error {

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

	dbCommand := dbCommandPool.Get().(*DBCommand)
	dbCommand.Reference = reference
	dbCommand.Record = record
	dbCommand.QueryStr = insertStr
	dbCommand.Args = recordDef.Values
	dbCommand.RecordDef = recordDef
	dbCommand.Tables = tables

	writer.commands <- dbCommand

	return nil
}
