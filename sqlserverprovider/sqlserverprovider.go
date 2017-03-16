package sqlserverprovider

import (
	"database/sql"
	"log"

	_ "github.com/denisenkom/go-mssqldb" //A mysql driver to allow database/sql understand the database
)

type SQLProvider struct {
	db      *sql.DB
	dbName  string
	perPage int
}

const meta_changelog_table = "meta_changelog"
const meta_data_table = "meta_data"

func New(dbType, dbConnectionString, dbName string, perPage int) (*SQLProvider, error) {
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
	err = createTriggers(mp.db, mp.dbName, meta_changelog_table)
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
