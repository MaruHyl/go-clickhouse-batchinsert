package batchinsert

type Logger interface {
	Log(keyAndValues ...interface{})
}

type nopLogger struct {
}

func (l nopLogger) Log(keyAndValues ...interface{}) {
}
