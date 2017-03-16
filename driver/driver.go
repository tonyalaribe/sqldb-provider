package driver

type SQLProvider interface {
	Initialize()
	Sync(func(string, string)) error
	ConfirmSync() error
}
