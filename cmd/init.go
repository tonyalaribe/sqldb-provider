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
		dbprovider.Initialize()
	},
}

func init() {
	RootCmd.AddCommand(initCmd)
}
