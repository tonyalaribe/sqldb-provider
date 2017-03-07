package cmd

import (
	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
	"github.com/robfig/cron"
	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the process to run scheduled queries to publish to Middle",
	Long: `This command starts a service that runs cron job scheduled queries
	against configured databases and publishes the results to Middle.`,
	Run: func(cmd *cobra.Command, args []string) {
		c := cron.New()
		//runs ever 2 minutes for debugging purposes. TODO: make the time configurable
		c.AddFunc("0 02 * * * *", func() {
			//syncDataToMiddle()
		})
		c.Start()

		//not sure if this is necessary.
		defer c.Stop()

	},
}

func init() {
	RootCmd.AddCommand(serveCmd)
}
