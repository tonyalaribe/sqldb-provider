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
	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
	"github.com/spf13/cobra"
)

// initCmd should initialize the provider by creating triggers, performing a first sync, and storing date of initial sync for so subsequent syncs can only publish data since the last sync
var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize Provider and Create Defaults",
	Long:  `This commandhould initialize the provider by creating triggers, performing a first sync, and storing date of initial sync for so subsequent syncs can only publish data since the last sync`,
	Run: func(cmd *cobra.Command, args []string) {
		createTriggersHandler()
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
}
