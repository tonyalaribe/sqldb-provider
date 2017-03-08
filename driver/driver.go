package driver

type Responses struct {
	Data map[string][]map[string]interface{}
}

type SQLProvider interface {
	Initialize()
	GetUpdatesForSync() (Responses, error)
	//GetDataForFirstSync() (Responses, error)
}
