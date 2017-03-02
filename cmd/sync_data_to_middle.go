package cmd

import (
	"database/sql"
	"encoding/json"
	"log"

	"github.com/spf13/viper"
	client "gitlab.com/middlefront/go-middle-client"
	"gitlab.com/middlefront/middle/core"
	"gitlab.com/middlefront/middle/props"
)

func syncDataToMiddle() {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)
	token := viper.GetString(tokenString)

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	defer db.Close()
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Fatalf("unable to ping database. Error: %+v", err.Error())
	}

	rows, err := db.Query("show TABLES")
	if err != nil {
		if err != nil {
			log.Fatalf("unable to get tables from database. Error: %+v", err.Error())
		}
	}
	var tables []string
	var tablename string
	for rows.Next() {
		err = rows.Scan(&tablename)
		if err != nil { /* error handling */
		}
		tables = append(tables, tablename)
	}
	log.Println(tables)

	// log.Printf("Data: %v", string(req))

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
		req.Token = token
		req.Batch = true
		req.Data = dat

		c := client.DefaultMiddleClient(cluster, natsURL, token)

		err = c.Publish(*req)
		if err != nil {
			log.Printf("unable to publish json to middle.  Error: %+v", err)
		}

	}
}
