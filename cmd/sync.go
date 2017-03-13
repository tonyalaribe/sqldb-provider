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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	client "gitlab.com/middlefront/go-middle-client"

	"github.com/spf13/cobra"
)

func performSync() error {
	responses, err := dbprovider.GetUpdatesForSync()
	if err != nil {
		log.Println(err)
	}

	if len(responses.Data) > 0 {
		for table, content := range responses.Data {

			var payload []byte
			payload, err = json.Marshal(content)
			if err != nil {
				log.Println(err)
			}
			req := &client.Batch{}
			req.Token = config.clientToken
			req.Type = table + ".upsert"
			req.TypeVersion = config.providerVersion //TODO:Increment Type version with each sync
			req.Provider = config.providerName
			req.Data = payload

			c := client.DefaultMiddleClient(config.natsURL, config.clientToken)

			err = c.Publish(*req)
			if err != nil {
				log.Printf("unable to publish json to middle.  Error: %+v", err)
			}

			/*** Debugging **/

			jsonval, err := json.Marshal(req)
			if err != nil {
				log.Println(err)
			}

			pretty := bytes.Buffer{}
			err = json.Indent(&pretty, jsonval, "", "\t")
			if err != nil {
				log.Println("JSON parse error: ", err)

			}

			err = ioutil.WriteFile("./uploaded_data/data"+table+".json", pretty.Bytes(), os.ModePerm)
			if err != nil {
				log.Println(err)
			}

			//log.Println(string(pretty.String()))

			/*** **/
		}

		//Confirm sync, so the date of sync is stored, to prevent republishing data.
		err = dbprovider.ConfirmSync()
		if err != nil {
			log.Println(err)
		}
		log.Println("Sync Performed and Confirmed successfully")
		//Improper error management. Sometime should be allocated to deciding how errors should be managed
		return nil
	} else if responses.DataString != "" {
		req := &client.Batch{}
		req.Token = config.clientToken
		req.Type = "dataupdates.upsert"
		req.TypeVersion = config.providerVersion //TODO:Increment Type version with each sync
		req.Provider = config.providerName
		req.Data = json.RawMessage(responses.DataString)

		c := client.DefaultMiddleClient(config.natsURL, config.clientToken)

		err = c.Publish(*req)
		if err != nil {
			log.Printf("unable to publish json to middle.  Error: %+v", err)
		}

		//Confirm sync, so the date of sync is stored, to prevent republishing data.
		err = dbprovider.ConfirmSync()
		if err != nil {
			log.Println(err)
		}
		log.Println("Sync Performed and Confirmed successfully")
	}
	return nil
}

// serveCmd represents the serve command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Publish data from linked datastore to Middle. Just once.",
	Long: `This command runs queries
	against configured databases and publishes the results to Middle.`,
	Run: func(cmd *cobra.Command, args []string) {
		err := performSync()
		if err != nil {
			log.Println(err)
		}
	},
}

func init() {
	RootCmd.AddCommand(syncCmd)
}
