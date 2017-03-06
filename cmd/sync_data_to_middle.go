package cmd

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database

	"github.com/spf13/viper"
	client "gitlab.com/middlefront/go-middle-client"
	"gitlab.com/middlefront/middle/core"
	"gitlab.com/middlefront/middle/props"

	"gitlab.com/middlefront/sqldb-provider/queries"
)

func createTriggers(db *sql.DB) {
	dbName := viper.GetString(databaseName)

	tablesAndColumns, err := queries.GetAllTablesAndColumns(db, dbName)
	if err != nil {
		log.Println(err)
	}

	//tablesAndPrimaryKeys := make(map[string][]string)

	// for tablename := range tablesAndColumns {
	// 	primaryKeysList := GetAllPrimaryKeysInTable(db, tablename)
	// 	tablesAndPrimaryKeys[tablename] = primaryKeysList
	// }

	//log.Println(tablesAndPrimaryKeys)

	//Create the changelog table where changes will be logged
	err = queries.CreateMetaChangeLogTable(db)
	if err != nil {
		log.Println(err)
	}

	//loop through all tables to set triggers for each of them.
	for tablename, columns := range tablesAndColumns {
		var primaryKeysAndValues string

		primaryKeysList, err := queries.GetAllPrimaryKeysInTable(db, tablename)
		if err != nil {
			log.Println(err)
		}

		for _, pks := range primaryKeysList {
			primaryKeysAndValues += "'" + pks + ":',OLD." + pks + ",',',"
		}
		primaryKeysAndValues += "''"

		var OldColumnValues string
		var NewColumnValues string

		for _, columnName := range columns {
			OldColumnValues += "'" + columnName + ":',OLD." + columnName + ",',',"
			NewColumnValues += "'" + columnName + ":',NEW." + columnName + ",',',"
		}
		OldColumnValues += "''"
		NewColumnValues += "''"

		err = queries.CreateDeleteTrigger(db, tablename, primaryKeysAndValues, OldColumnValues)
		if err != nil {
			log.Println(err)
		}

		err = queries.CreateUpdateTrigger(db, tablename, primaryKeysAndValues, OldColumnValues, NewColumnValues)
		if err != nil {
			log.Println(err)
		}
	}
}

func createTriggersHandler() error {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	defer db.Close()
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Printf("unable to ping database. Error: %+v", err.Error())
		return err
	}
	createTriggers(db)
	return nil
}

func syncDataToMiddle() error {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)
	clientToken := viper.GetString(clientTokenString)
	cluster := viper.GetString(props.NatsClusterProp)
	natsURL := viper.GetString(props.NatsURLProp)

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	defer db.Close()
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Printf("unable to ping database. Error: %+v", err.Error())
		return err
	}

	tables, err := queries.GetAllTables(db)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return err
	}

	//decalared outside the loop to prevent excessive heap allocations
	var dat []map[string]interface{}

	for _, table := range tables {
		tableJSON, err := getJSON(db, "select * from "+table)
		if err != nil {
			log.Printf("unable to convert table data to json. Error: %+v", err)
		}

		err = json.Unmarshal([]byte(tableJSON), &dat)
		if err != nil {
			log.Printf("unable to unmarshall json to []map[string]interface. Error: %+v", err)
		}
		req := &core.PublishRequest{}
		req.Token = clientToken
		req.Type = table + ".upsert"
		req.TypeVersion = "1.0" //TODO:Increment Type version with each sync
		req.Data = dat

		c := client.DefaultMiddleClient(cluster, natsURL, clientToken)

		err = c.Publish(*req)
		if err != nil {
			log.Printf("unable to publish json to middle.  Error: %+v", err)
		}

	}
	return nil
}
