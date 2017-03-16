package cmd

import (
	"encoding/json"
	"log"

	client "gitlab.com/middlefront/go-middle-client"

	"github.com/spf13/cobra"
)

func performSync() error {
	err := dbprovider.Sync(func(data string, requestType string) {
		req := &client.Batch{}
		req.Token = config.clientToken
		req.Type = requestType                   //table + ".upsert"
		req.TypeVersion = config.providerVersion //TODO:Increment Type version with each sync
		req.Provider = config.providerName
		req.Data = json.RawMessage(data)

		c := client.DefaultMiddleClient(config.natsURL, config.clientToken)

		err := c.Publish(*req)
		if err != nil {
			log.Printf("unable to publish json to middle.  Error: %+v", err)
		}

		//Confirm sync, so the date of sync is stored, to prevent republishing data.
		err = dbprovider.ConfirmSync()
		if err != nil {
			log.Println(err)
		}
		log.Println("Sync Performed and Confirmed successfully")
	})
	if err != nil {
		log.Println(err)
		return err
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
