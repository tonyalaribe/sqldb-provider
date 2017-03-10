package sqlserverprovider

import (
	"database/sql"
	"log"

	_ "github.com/denisenkom/go-mssqldb" //A mysql driver to allow database/sql understand the database
	"gitlab.com/middlefront/sqldb-provider/driver"
)

type MySQLProvider struct {
	db     *sql.DB
	dbName string
}

const meta_changelog_table = "meta_changelog"
const meta_data_table = "meta_data"

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

	log.Println("pinged successfully")
	return &mp, nil
}

func (mp *MySQLProvider) Initialize() {
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

func (mp *MySQLProvider) GetUpdatesForSync() (driver.Responses, error) {
	log.Println(mp)

	resp := driver.Responses{}
	var err error

	lastSync, err := getLastSync(mp.db, meta_data_table)
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
	resp, err = mp.getDataForRegularSync(lastSync)
	if err != nil {
		log.Println(err)
	}

	return resp, nil
}

func (mp *MySQLProvider) ConfirmSync() error {
	err := setLastSyncToNow(mp.db, meta_data_table)
	if err != nil {
		return err
	}
	return nil
}
