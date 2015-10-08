package lease

var (
	globalLeaseLogger LeaseLogger
)

// LeaseLogger is the interface for the internal logger.
type LeaseLogger interface {
	LogInfoMessage(message string, keyvals ...interface{})
	LogErrorMessage(message string, keyvals ...interface{})
}

type noopLogger struct{}

func (_ *noopLogger) LogInfoMessage(message string, keyvals ...interface{})  {}
func (_ *noopLogger) LogErrorMessage(message string, keyvals ...interface{}) {}

func init() {
	globalLeaseLogger = &noopLogger{}
}

// Set the logger implementation used within Lease
func SetLogger(logger LeaseLogger) {
	globalLeaseLogger = logger
}
