package driver

//I'm not sure these might be the ideal response interface.
type Responses struct {
	Data       map[string][]map[string]interface{}
	DataString string
}

type SQLProvider interface {
	Initialize()
	GetUpdatesForSync() (Responses, error)
	ConfirmSync() error
}
