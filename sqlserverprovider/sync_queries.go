package sqlserverprovider

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"gitlab.com/middlefront/sqldb-provider/driver"
)

func (mp *MySQLProvider) getDataForRegularSync(lastSync string) (driver.Responses, error) {
	resp := driver.Responses{}
	resp.Data = make(map[string][]map[string]interface{})

	tableJSON, err := getJSON(mp.db, "SELECT * FROM "+meta_changelog_table+" WHERE ActionDate > '"+lastSync+"'")
	if err != nil {
		log.Printf("unable to convert table data to json. Error: %+v", err)
	}

	resp.DataString = tableJSON

	return resp, nil
}

func (mp *MySQLProvider) getDataForFirstSync() (driver.Responses, error) {
	resp := driver.Responses{}
	resp.Data = make(map[string][]map[string]interface{})

	tables, err := getAllTables(mp.db, mp.dbName)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return resp, err
	}
	//decalared outside the loop to prevent excessive heap allocations
	var dat []map[string]interface{}

	for _, table := range tables {
		if table == meta_changelog_table || table == meta_data_table {
			continue
		}

		tableJSON, err := getJSON(mp.db, "select * from "+table)
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

func setLastSyncToNow(db *sql.DB, metaDataTable string) error {
	query := fmt.Sprintf(`UPDATE %s SET
			 DataValue = GETDATE()
			 WHERE DataKey='last_sync';`, metaDataTable)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func getLastSync(db *sql.DB, metaDataTable string) (string, error) {
	query := fmt.Sprintf(`SELECT DataValue FROM %s
			 WHERE DataKey='last_sync'
		;`, metaDataTable)

	row := db.QueryRow(query)

	var err error
	lastSync := ""
	err = row.Scan(&lastSync)
	if err != nil {
		log.Println(err)
		//return err
	}
	log.Println(lastSync)
	return lastSync, nil
}
