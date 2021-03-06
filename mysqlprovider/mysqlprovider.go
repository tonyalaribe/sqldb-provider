package mysqlprovider

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

type SQLProvider struct {
	db             *sql.DB
	dbName         string
	perPage        int //perPage should be the number of rows to be be published at a time, to prevent using up too many resources or reaching maximum message size for middle servers.
	excludedTables []string
}

const meta_changelog_table = "meta_changelog"
const meta_data_table = "meta_data"

func New(dbType, dbConnectionString, dbName string, perPage int, excludedTables []string) (*SQLProvider, error) {
	var mp SQLProvider

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
	mp.perPage = perPage
	mp.excludedTables = excludedTables

	log.Println("pinged database successfully")
	return &mp, nil
}

func (mp *SQLProvider) Initialize() {
	var err error
	err = createMetaChangeLogTable(mp.db, meta_changelog_table)
	if err != nil {
		log.Println(err)
	}
	err = createMetaDataTable(mp.db, meta_data_table)
	if err != nil {
		log.Println(err)
	}
	err = createTriggers(mp.db, mp.dbName, meta_changelog_table, mp.excludedTables)
	if err != nil {
		log.Println(err)
	}

}

func (mp *SQLProvider) Sync(syncFunc func(string, string)) error {
	var err error

	lastSync, err := getLastSync(mp.db, meta_data_table)
	if err != nil {
		log.Println(err)
	}
	//
	if lastSync == "" {
		err = mp.performFirstSync(syncFunc)
		if err != nil {
			log.Println(err)
		}
		return nil
	}
	err = mp.performRegularSync(lastSync, syncFunc)
	if err != nil {
		log.Println(err)
	}

	return nil
}

func (mp *SQLProvider) ConfirmSync() error {
	err := setLastSyncToNow(mp.db, meta_data_table)
	if err != nil {
		return err
	}
	return nil
}

//Use within performFirstSync to makesure table does not exist in exclude list, also usefull when dealing with triggers
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
