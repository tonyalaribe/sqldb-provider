package cmd

func syncDataToMiddle() error {
	// clientToken := viper.GetString(clientTokenString)
	// cluster := viper.GetString(props.NatsClusterProp)
	// natsURL := viper.GetString(props.NatsURLProp)
	//
	// responses, err := dbprovider.GetDataForFirstSync()
	// if err != nil {
	// 	log.Println(err)
	// }
	// log.Println(responses)
	return nil
}

// func syncDataToMiddlexxx() error {
// 	dbType := viper.GetString(databaseType)
// 	dbConnectionString := viper.GetString(dataBaseConnectionString)
//
// 	//decalared outside the loop to prevent excessive heap allocations
// 	var dat []map[string]interface{}
//
// 	for _, table := range tables {
// 		tableJSON, err := getJSON(db, "select * from "+table)
// 		if err != nil {
// 			log.Printf("unable to convert table data to json. Error: %+v", err)
// 		}
//
// 		err = json.Unmarshal([]byte(tableJSON), &dat)
// 		if err != nil {
// 			log.Printf("unable to unmarshall json to []map[string]interface. Error: %+v", err)
// 		}
// 		req := &core.PublishRequest{}
// 		req.Token = clientToken
// 		req.Type = table + ".upsert"
// 		req.TypeVersion = "1.0" //TODO:Increment Type version with each sync
// 		req.Data = dat
//
// 		c := client.DefaultMiddleClient(cluster, natsURL, clientToken)
//
// 		err = c.Publish(*req)
// 		if err != nil {
// 			log.Printf("unable to publish json to middle.  Error: %+v", err)
// 		}
//
// 	}
// 	return nil
// }
