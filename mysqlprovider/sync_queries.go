package mysqlprovider

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

func (mp *SQLProvider) performRegularSync(lastSync string, sync func(string, string)) error {
	perPage := mp.perPage
	countRow := mp.db.QueryRow("SELECT count(*) FROM " + meta_changelog_table + " WHERE ActionDate > '" + lastSync + "'")

	var count int
	err := countRow.Scan(&count)
	if err != nil {
		log.Println(err)
	}

	pages := count / perPage
	for i := 0; i < pages; i++ {
		startInt := i * perPage
		start := strconv.Itoa(startInt)
		end := strconv.Itoa(startInt + perPage)

		tableJSON, err := getJSON(mp.db, "SELECT * FROM "+meta_changelog_table+" WHERE ActionDate > '"+lastSync+"' "+" limit "+start+","+end)
		if err != nil {
			log.Printf("unable to convert table data to json. Error: %+v", err)
		}

		sync(tableJSON, meta_changelog_table)
	}

	return nil
}

func (mp *SQLProvider) performFirstSync(sync func(string, string)) error {
	perPage := mp.perPage
	tables, err := getAllTables(mp.db)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return err
	}

	for _, table := range tables {
		if table == meta_changelog_table || table == meta_data_table {
			continue
		}

		countRow := mp.db.QueryRow("select count(*) from " + table)

		var count int
		err = countRow.Scan(&count)
		if err != nil {
			log.Println(err)
		}

		pages := count / perPage
		for i := 0; i < pages; i++ {
			startInt := i * perPage
			start := strconv.Itoa(startInt)
			end := strconv.Itoa(startInt + perPage)
			tableJSON, err := getJSON(mp.db, "select * from "+table+" limit "+start+","+end)
			if err != nil {
				log.Printf("unable to convert table data to json. Error: %+v", err)
			}
			sync(tableJSON, table)
		}
	}
	return nil
}

func setLastSyncToNow(db *sql.DB, metaDataTable string) error {
	query := fmt.Sprintf(`UPDATE %s SET
			 DataValue = NOW()
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
