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
)

//getTables returns all tables in a given database, and an error, if unsuccessful
func getTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("show TABLES")
	if err != nil {
		if err != nil {
			log.Fatalf("unable to get tables from database. Error: %+v", err.Error())
			return tables, err
		}
	}

	var tablename string
	for rows.Next() {
		err = rows.Scan(&tablename)
		if err != nil { /* error handling. Not sure what kind of errors would return nil when rows.Next() had returned true. TODO: handle error appropriately */
			log.Fatalf("unable to get tables from database. Error: %+v", err.Error())
		}
		tables = append(tables, tablename)
	}

	return tables, err
}

func syncDataToMiddle() error {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)
	clientToken := viper.GetString(clientTokenString)

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

	tables, err := getTables(db)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return err
	}

	//decalared outside the loop to prevent excessive heap allocations
	cluster := viper.GetString(props.NatsClusterProp)
	natsURL := viper.GetString(props.NatsURLProp)
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
