package engine

import "time"

type Mode string

const (
	ModeSerial   Mode = "serial"
	ModeParallel Mode = "parallel"
)

type CommonOptions struct {
	DBAddr string
	DBName string

	Mode Mode

	Concurrency     int
	TablesToCreate  int
	MySQLCompatible bool
	TestTime        time.Duration

	Output string

	Txn                  bool
	RC                   bool
	Prepare              bool
	CheckDDLExtraTimeout time.Duration
}
