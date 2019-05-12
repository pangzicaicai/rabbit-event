package rabbitevent

import (
	"log"
	"os"
)

var (
	defaultLogger = Logger{log.New(os.Stdout, "\r\n", 0)}
)

const (
	logModeEnable  = 1
	logModeDisable = 0
)

type logger interface {
	Print(v ...interface{})
}

type LogWriter interface {
	Println(v ...interface{})
}

// Logger default logger
type Logger struct {
	LogWriter
}

// Print format & print log
func (logger Logger) Print(values ...interface{}) {
	logger.LogWriter.Println(values...)
}
