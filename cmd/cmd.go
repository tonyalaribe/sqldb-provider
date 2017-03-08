package cmd

import (
	"fmt"
	"log"
	"os"

	"gitlab.com/middlefront/sqldb-provider/driver"
	"gitlab.com/middlefront/sqldb-provider/mysqlprovider"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile            string
	dbConnectionString string
	dbType             string
	clientTokenString  string
	dbName             string
	dbprovider         driver.SQLProvider
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "sqldb-provider",
	Short: "Publish data to middle server",
	Long: `sqldb-provider makes it possible to publish data from an sql database to the middle server either once, or at intervals
	`,
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.sqldb-provider.yaml)")

}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

	viper.SetConfigName(".sqldb-provider") // name of config file (without extension)
	viper.AddConfigPath(".")               // The apps root root directory as first search path
	viper.AddConfigPath("$HOME")           // adding home directory as second search path
	viper.AutomaticEnv()                   // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	//This initialization is placed here, because initConfig is a callback that is called after cobra has parsed the config file and other variables. The other ideal location would have been the init function, but the init function is called before the config has been parsed, and hence the absense of the needed variables.

	dbConnectionString = viper.GetString("database-connection-string")
	dbType = viper.GetString("database-type")
	clientTokenString = viper.GetString("client-token")
	dbName = viper.GetString("database-name")

	log.Println(dbType)
	log.Println(dbConnectionString)
	log.Println(dbName)

	mysqldb, err := mysqlprovider.New(dbType, dbConnectionString, dbName)
	if err != nil {
		log.Println(err)
	}
	dbprovider = driver.SQLProvider(mysqldb)
}
