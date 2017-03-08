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
	"log"

	"gitlab.com/middlefront/middle/core"

	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Publish data from linked datastore to Middle. Just once.",
	Long: `This command runs queries
	against configured databases and publishes the results to Middle.`,
	Run: func(cmd *cobra.Command, args []string) {
		responses, err := dbprovider.GetUpdatesForSync()
		if err != nil {
			log.Println(err)
		}
		for table, content := range responses.Data {
			log.Printf("%+v", content)
			req := &core.PublishRequest{}
			req.Token = config.clientToken //global variable TODO: global variables should be grouped in a struct for ease of use and identification
			req.Type = table + ".upsert"
			req.TypeVersion = "1.0" //TODO:Increment Type version with each sync
			//req.Data = dat

			// c := client.DefaultMiddleClient(config.cluster, config.natsURL, config.clientToken)
			//
			// err = c.Publish(*req)
			// if err != nil {
			// 	log.Printf("unable to publish json to middle.  Error: %+v", err)
			// }
		}
	},
}

func init() {
	RootCmd.AddCommand(syncCmd)
}
