//Package sqlserverprovider package exports database access queries, to help decrease clutter in the packages using these queries.
package sqlserverprovider

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

//getAllTables returns all tables in a given database, and an error, if unsuccessful
func getAllTables(db *sql.DB, dbName string) ([]string, error) {
	var tables []string

	query := fmt.Sprintf("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='%s';", dbName)

	rows, err := db.Query(query)
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

//getAllTablesAndColumns returns a map of tables to a list of curresponding columns
func getAllTablesAndColumns(db *sql.DB, dbName string) (map[string][]string, error) {
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
			//return tablesAndColumns, err
		}
	}

	return tablesAndColumns, nil
}

//getAllPrimaryKeysInTable returns a map of tables to a list of curresponding primary keys
func getAllPrimaryKeysInTable(db *sql.DB, tablename string) ([]string, error) {

	query := fmt.Sprintf(`
			SELECT column_name as PRIMARYKEYCOLUMN
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
			INNER JOIN
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
				ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
				TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
				KU.table_name='%s'
				ORDER BY KU.ORDINAL_POSITION;
			`, tablename)

	rows, err := db.Query(query)
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
	return primaryKeysList, nil
}

// createMetaChangeLogTable : Creates a table to store all changes, old and new, alongside the
// ID (Primary Key) | TableName | PrimaryKeys (x:xval,y:yval) | OldColumnValues (x:xval,y:yval) | NewColumnValue(x:xval,y:yval)  | TriggerType ( I|U|D ) | ActionDate
func createMetaChangeLogTable(db *sql.DB, metaTableName string) error {

	query := fmt.Sprintf(`CREATE TABLE %s(
		 ID int NOT NULL identity(1, 1) primary key,
		 TableName varchar(255),
		 PrimaryKeys varchar(255),
		 OldColumnValue NTEXT,
		 NewColumnValue NTEXT,
		 TriggerType varchar(10),
		 ActionDate datetime NOT NULL DEFAULT GETDATE()
	);`, metaTableName)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func createMetaDataTable(db *sql.DB, metaDataTable string) error {
	query := fmt.Sprintf(`CREATE TABLE %s(
			 DataKey varchar(30) NOT NULL,
			 DataValue varchar(255),
			 PRIMARY KEY(DataKey)
		);`, metaDataTable)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// return err
	}
	query = fmt.Sprintf(`INSERT INTO %s (DataKey)
			 VALUES ('last_sync');`, metaDataTable)

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		// return err
	}
	return nil
}
