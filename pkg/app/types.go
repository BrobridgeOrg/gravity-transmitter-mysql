package app

import (
	"github.com/BrobridgeOrg/gravity-transmitter-mysql/pkg/database"
)

type App interface {
	GetWriter() database.Writer
}
