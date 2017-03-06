package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	var query string

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
	}

	log.Println(tablesAndColumns)

	// SELECT KU.table_name as TABLENAME,
	// column_name as PRIMARYKEYCOLUMN
	// FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
	// INNER JOIN
	// 	INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
	// 	ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
	// 	TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
	// 	KU.table_name='%s'
	// 	ORDER BY KU.ORDINAL_POSITION;

	tablesAndPrimaryKeys := make(map[string][]string)

	for tablename := range tablesAndColumns {
		query = fmt.Sprintf(`
			SELECT column_name as PRIMARYKEYCOLUMN
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
			INNER JOIN
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
				ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
				TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
				KU.table_name='%s'
				ORDER BY KU.ORDINAL_POSITION;
			`, tablename)
		rows, err = db.Query(query)
		if err != nil {
			log.Fatalf("unable to get colums in tables. Error: %+v", err.Error())
		}
		var primarykeycolumn string
		primaryKeysMap := make(map[string]interface{})
		for rows.Next() {
			err = rows.Scan(&primarykeycolumn)
			if err != nil {
				log.Println(err)
			}
			primaryKeysMap[primarykeycolumn] = nil
		}

		var primaryKeysList []string
		for primaryKey := range primaryKeysMap {
			primaryKeysList = append(primaryKeysList, primaryKey)
		}

		tablesAndPrimaryKeys[tablename] = primaryKeysList

	}

	log.Println(tablesAndPrimaryKeys)

	// TODO: Create a table to store all changes, old and new, alongside the
	// ID | KEYS (x:xval,y:yval) | TABLE_NAME | TRIGGER_TYPE

	query = fmt.Sprintf(`CREATE TABLE meta_changelog(
	   ID int NOT NULL AUTO_INCREMENT,
		 TableName varchar(255),
		 PrimaryKeys varchar(255),
		 OldColumnValue varchar(255),
		 NewColumnValue varchar(255),
		 TriggerType varchar(10),
		 ActionDate datetime NOT NULL DEFAULT NOW(),
	   PRIMARY KEY(ID)
	);`)
	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}

	for tablename, columns := range tablesAndColumns {
		var pkSQLString string
		log.Println(tablesAndPrimaryKeys[tablename])
		for _, pks := range tablesAndPrimaryKeys[tablename] {
			pkSQLString += "'" + pks + ":',OLD." + pks + ",',',"
		}
		pkSQLString += "''"

		var OldColumnValues string
		var NewColumnValues string

		for _, columnName := range columns {
			OldColumnValues += "'" + columnName + ":',OLD." + columnName + ",',',"
			NewColumnValues += "'" + columnName + ":',NEW." + columnName + ",',',"
		}
		OldColumnValues += "''"
		NewColumnValues += "''"

		query = fmt.Sprintf(`
			DROP TRIGGER %s_delete_trigger;`, tablename)
		log.Println(query)
		_, err = db.Query(query)
		if err != nil {
			log.Println(err)
		}
		query = fmt.Sprintf(`
			DROP TRIGGER %s_delete_trigger;
			CREATE TRIGGER %s_delete_trigger
		AFTER DELETE ON %s
		FOR EACH ROW
  	INSERT INTO meta_changelog (TableName, PrimaryKeys, OldColumnValue,  TriggerType )
      VALUES ('%s',CONCAT(%s),CONCAT(%s),'D');`, tablename, tablename, tablename, tablename, pkSQLString, OldColumnValues)

		log.Println(query)
		_, err = db.Query(query)
		if err != nil {
			log.Println(err)
		}

		query = fmt.Sprintf(`
			DROP TRIGGER %s_delete_trigger;`, tablename)
		log.Println(query)
		_, err = db.Query(query)
		if err != nil {
			log.Println(err)
		}
		query = fmt.Sprintf(`
			DROP TRIGGER %s_update_trigger;
			CREATE TRIGGER %s_update_trigger
		AFTER UPDATE ON %s
		FOR EACH ROW
		INSERT INTO meta_changelog (TableName, PrimaryKeys, OldColumnValue, NewColumnValue, TriggerType )
			VALUES ('%s',CONCAT(%s),CONCAT(%s),CONCAT(%s), 'U');`, tablename, tablename, tablename, tablename, pkSQLString, OldColumnValues, NewColumnValues)
		log.Println(query)
		_, err = db.Query(query)
		if err != nil {
			log.Println(err)
		}
	}

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
