package cmd

import (
	"fmt"
	"log"
	"os"

	"gitlab.com/middlefront/sqldb-provider/driver"
	"gitlab.com/middlefront/sqldb-provider/mysqlprovider"
	"gitlab.com/middlefront/sqldb-provider/sqlserverprovider"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

//Config Holds all the serialised yaml config variables. Grouping the variables here should make maintainance easier (namespacing too)
type Config struct {
	dbConnectionString string
	dbType             string
	clientToken        string
	providerName       string
	providerVersion    string
	dbName             string
	cluster            string
	natsURL            string
}

var (
	cfgFile string

	dbprovider driver.SQLProvider

	config Config //global variable representing config variables.
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

	config.dbConnectionString = viper.GetString("database-connection-string")
	config.dbType = viper.GetString("database-type")
	config.clientToken = viper.GetString("client-token")
	config.providerName = viper.GetString("provider-name")
	config.providerVersion = viper.GetString("provider-version")
	config.dbName = viper.GetString("database-name")

	config.cluster = viper.GetString("nats-cluster")
	config.natsURL = viper.GetString("nats-url")

	switch config.dbType {
	case "mysql":
		sqldb, err := mysqlprovider.New(config.dbType, config.dbConnectionString, config.dbName)
		if err != nil {
			log.Fatal(err)
		}
		dbprovider = driver.SQLProvider(sqldb)
		break
	case "sqlserver":
		sqldb, err := sqlserverprovider.New(config.dbType, config.dbConnectionString, config.dbName)
		if err != nil {
			log.Fatal(err)
		}
		dbprovider = driver.SQLProvider(sqldb)
	default:
		log.Fatal("unknown database Type: " + config.dbType)
	}
}
