package cmd

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database

	"github.com/spf13/viper"
	client "gitlab.com/middlefront/go-middle-client"
	"gitlab.com/middlefront/middle/core"
	"gitlab.com/middlefront/middle/props"
)

//getTables returns all tables in a given database, and an error, if unsuccessful
func getTables(db *sql.DB) ([]string, error) {
	var tables []string
	rows, err := db.Query("show TABLES")
	if err != nil {
		log.Fatalf("unable to get tables from database. Error: %+v", err.Error())
		return tables, err
	}

	var tablename string
	for rows.Next() {
		err = rows.Scan(&tablename)
		if err != nil { /* error handling. Not sure what kind of errors would return nil when rows.Next() had returned true. TODO: handle error appropriately */
			log.Fatalf("unable to get tables from database. Error: %+v", err.Error())
		}
		tables = append(tables, tablename)
	}

	return tables, err
}

func createTriggers(db *sql.DB) {
	dbName := viper.GetString(databaseName)

	rows, err := db.Query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + dbName + "';")
	if err != nil {
		log.Fatalf("unable to get colums in tables. Error: %+v", err.Error())
	}

	var data = struct {
		TableSchema   string
		TableName     string
		ColumnName    string
		ColumnDefault string
		DataType      string
	}{}

	tablesAndColumns := map[string][]string{}

	for rows.Next() {
		err = rows.Scan(&data.TableSchema, &data.TableName, &data.ColumnName, &data.ColumnDefault, &data.DataType)

		columns := tablesAndColumns[data.TableName]
		columns = append(columns, data.ColumnName)
		tablesAndColumns[data.TableName] = columns
		if err != nil {
			/* error handling. Not sure what kind of errors would return nil when rows.Next() had returned true. TODO: handle error appropriately */
			log.Printf("unable to get tables from database. Error: %+v", err.Error())
		}
		log.Println(data)
	}
	log.Println(tablesAndColumns)
	//
	//
	// 	db.Query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + dbName + "';")
	//
	// 	select CCU.CONSTRAINT_NAME, CCU.COLUMN_NAME
	// 	from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC
	// 	inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as CCU
	// 	    on TC.CONSTRAINT_CATALOG = CCU.CONSTRAINT_CATALOG
	// 	    and TC.CONSTRAINT_SCHEMA = CCU.CONSTRAINT_SCHEMA
	// 	    and TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
	// 	where TC.CONSTRAINT_CATALOG = 'MyCatalogName'
	// 	and TC.CONSTRAINT_SCHEMA = 'MySchemaName'
	// 	and TC.TABLE_NAME = 'city'
	// 	and TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
	//
	//
	// 	SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
	// 	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
	// 	INNER JOIN
	// 	    INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
	// 	          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
	// 	             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
	// 	             KU.table_name='city'
	// 	ORDER BY KU.ORDINAL_POSITION;
	//
	// 	SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
	// 	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
	// 	INNER JOIN
	// 			INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
	// 						ON TC.CONSTRAINT_TYPE = 'UNIQUE' AND
	// 							 TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
	// 							 KU.table_name='city'
	// 	ORDER BY KU.ORDINAL_POSITION;
	//
	// 	KU.TABLE_NAME,
	// 	select *
	// 	from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC
	// 	where TC.CONSTRAINT_SCHEMA = 'world'
	// 	and TC.TABLE_NAME = 'city'
	// 	and TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
	//
	// 	SELECT     CCU.CONSTRAINT_NAME, CCU.COLUMN_NAME
	// 	FROM         INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN
	// 	                      INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON TC.CONSTRAINT_CATALOG = CCU.CONSTRAINT_CATALOG AND
	// 	                      TC.CONSTRAINT_SCHEMA = CCU.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
	// 	WHERE     (TC.TABLE_NAME = 'city')
	//
	// CREATE TRIGGER insert_data AFTER INSERT ON
	// 	CREATE TRIGGER ins_sum BEFORE INSERT ON account
	//     -> FOR EACH ROW SET @sum = @sum + NEW.amount;
	//
	// 		CREATE TRIGGER ins_transaction BEFORE INSERT ON account
	//     -> FOR EACH ROW PRECEDES ins_sum
	//     -> SET
	//     -> @deposits = @deposits + IF(NEW.amount>0,NEW.amount,0),
	//     -> @withdrawals = @withdrawals + IF(NEW.amount<0,-NEW.amount,0);
}

func createTriggersHandler() error {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	defer db.Close()
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Printf("unable to ping database. Error: %+v", err.Error())
		return err
	}
	createTriggers(db)
	return nil
}

func syncDataToMiddle() error {
	dbType := viper.GetString(databaseType)
	dbConnectionString := viper.GetString(dataBaseConnectionString)
	clientToken := viper.GetString(clientTokenString)
	cluster := viper.GetString(props.NatsClusterProp)
	natsURL := viper.GetString(props.NatsURLProp)

	db, err := sql.Open(dbType, dbConnectionString)
	if err != nil {
		log.Println(err.Error())
	}
	defer db.Close()
	// make sure connection is available
	err = db.Ping()
	if err != nil {
		log.Printf("unable to ping database. Error: %+v", err.Error())
		return err
	}

	tables, err := getTables(db)
	if err != nil {
		log.Printf("unable to get dataBases. Error: %+v", err.Error())
		return err
	}

	//decalared outside the loop to prevent excessive heap allocations
	var dat []map[string]interface{}

	for _, table := range tables {
		tableJSON, err := getJSON(db, "select * from "+table)
		if err != nil {
			log.Printf("unable to convert table data to json. Error: %+v", err)
		}

		err = json.Unmarshal([]byte(tableJSON), &dat)
		if err != nil {
			log.Printf("unable to unmarshall json to []map[string]interface. Error: %+v", err)
		}
		req := &core.PublishRequest{}
		req.Token = clientToken
		req.Type = table + ".upsert"
		req.TypeVersion = "1.0" //TODO:Increment Type version with each sync
		req.Data = dat

		c := client.DefaultMiddleClient(cluster, natsURL, clientToken)

		err = c.Publish(*req)
		if err != nil {
			log.Printf("unable to publish json to middle.  Error: %+v", err)
		}

	}
	return nil
}
