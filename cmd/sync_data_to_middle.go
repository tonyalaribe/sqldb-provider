package cmd

import _ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database

//
//
// func syncDataToMiddle() error {
// 	dbType := viper.GetString(databaseType)
// 	dbConnectionString := viper.GetString(dataBaseConnectionString)
// 	clientToken := viper.GetString(clientTokenString)
// 	cluster := viper.GetString(props.NatsClusterProp)
// 	natsURL := viper.GetString(props.NatsURLProp)
//
// 	db, err := sql.Open(dbType, dbConnectionString)
// 	if err != nil {
// 		log.Println(err.Error())
// 	}
// 	defer db.Close()
// 	// make sure connection is available
// 	err = db.Ping()
// 	if err != nil {
// 		log.Printf("unable to ping database. Error: %+v", err.Error())
// 		return err
// 	}
//
// 	tables, err := queries.GetAllTables(db)
// 	if err != nil {
// 		log.Printf("unable to get dataBases. Error: %+v", err.Error())
// 		return err
// 	}
//
// 	return nil
// }
