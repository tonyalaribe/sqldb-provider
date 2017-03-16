package sqlserverprovider

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
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

//performFirstSync is different from its counterpart in mySQL, because of the use of ROW_NUMBER() which is abscent in mysql. There is also the need to order by primary keys for the query to work, unlike with mysql where the query works without order by.
func (mp *SQLProvider) performFirstSync(sync func(string, string)) error {
	perPage := mp.perPage
	tables, err := getAllTables(mp.db, mp.dbName)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return err
	}

	for _, table := range tables {

		if table == meta_changelog_table || table == meta_data_table || contains(mp.excludedTables, table) {
			continue
		}

		primaryKeysList, err := getAllPrimaryKeysInTable(mp.db, table)
		if err != nil {
			log.Println(err)
		}
		primaryKeysCSV := strings.Join(primaryKeysList, ",")

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

			query := fmt.Sprintf(`SELECT * FROM (
  				SELECT *, ROW_NUMBER() OVER (ORDER BY %[4]s) as row FROM %[1]s
 					) a WHERE row > %[2]s and row <= %[3]s`, table, start, end, primaryKeysCSV)

			tableJSON, err := getJSON(mp.db, query)

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
