package mysqlprovider

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
	"gitlab.com/middlefront/sqldb-provider/driver"
)

type MySQLProvider struct {
	db     *sql.DB
	dbName string
}

func New(dbType, dbConnectionString, dbName string) (*MySQLProvider, error) {
	var mp MySQLProvider

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Printf("unable to ping database. Error: %+v", err.Error())
		return &mp, err
	}
	mp.db = db
	mp.dbName = dbName

	return &mp, nil
}

func (mp *MySQLProvider) Initialize() {
	var err error
	err = createMetaChangeLogTable(mp.db, "meta_changelog")
	if err != nil {
		log.Println(err)
	}
	err = createMetaDataTable(mp.db, "meta_data")
	if err != nil {
		log.Println(err)
	}
	err = createTriggers(mp.db, mp.dbName, "meta_changelog")
	if err != nil {
		log.Println(err)
	}

}

func (mp *MySQLProvider) GetUpdatesForSync() (driver.Responses, error) {
	log.Println(mp)

	resp := driver.Responses{}
	var err error

	lastSync, err := getLastSync(mp.db, "meta_data")
	if err != nil {
		log.Println(err)
	}

	if lastSync == "" {
		resp, err = mp.getDataForFirstSync()
		if err != nil {
			log.Println(err)
		}
		return resp, nil
	}

	log.Println(lastSync)
	return resp, nil
}

func (mp *MySQLProvider) getDataForFirstSync() (driver.Responses, error) {
	resp := driver.Responses{}
	resp.Data = make(map[string][]map[string]interface{})

	tables, err := getAllTables(mp.db)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return resp, err
	}
	//decalared outside the loop to prevent excessive heap allocations
	var dat []map[string]interface{}

	for _, table := range tables {
		tableJSON, err := getJSON(mp.db, "select * from "+table+" limit 1")
		if err != nil {
			log.Printf("unable to convert table data to json. Error: %+v", err)
		}

		err = json.Unmarshal([]byte(tableJSON), &dat)
		if err != nil {
			log.Printf("unable to unmarshall json to []map[string]interface. Error: %+v", err)
		}

		resp.Data[table] = dat
	}
	return resp, nil
}
