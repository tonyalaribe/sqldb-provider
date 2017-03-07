package mysqlprovider

import (
	"database/sql"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
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
	createTriggers(mp.db, mp.dbName, "meta_changelog")
}

func (mp *MySQLProvider) GetUpdatesForSync() {
	log.Println(mp)
}
