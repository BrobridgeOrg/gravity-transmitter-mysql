package instance

import (
	database "github.com/BrobridgeOrg/gravity-transmitter-mysql/pkg/database"
)

func (a *AppInstance) initWriter() error {
	return a.writer.Init()
}

func (a *AppInstance) GetWriter() database.Writer {
	return database.Writer(a.writer)
}
