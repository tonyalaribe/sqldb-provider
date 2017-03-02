// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"log"

	client "gitlab.com/middlefront/go-middle-client"
	"gitlab.com/middlefront/middle/core"
	"gitlab.com/middlefront/middle/props"

	"database/sql"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const dataBaseConnectionString = "database-connection-string"
const databaseType = "database-type"
const tokenString = "token"

func serveCommandHandler(cmd *cobra.Command, args []string) {
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
	log.Println(cluster)
	log.Println(natsURL)
	for _, table := range tables {

		tableJSON, err := getJSON(db, "select * from "+table)
		if err != nil {
			log.Printf("unable to convert table data to json. Error: %+v", err)
		}

		json.Unmarshal([]byte(tableJSON), &dat)
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

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the process to run scheduled queries to publish to Middle",
	Long: `This command starts a service that runs cron job scheduled queries
	against configured databases and publishes the results to Middle.`,
	Run: serveCommandHandler,
}

func init() {
	RootCmd.AddCommand(serveCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// serveCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// serveCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

}
