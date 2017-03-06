//Package queries package exports database access queries, to help decrease clutter in the packages using these queries.
package queries

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

//GetAllTables returns all tables in a given database, and an error, if unsuccessful
func GetAllTables(db *sql.DB) ([]string, error) {
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

//GetAllTablesAndColumns returns a map of tables to a list of curresponding columns
func GetAllTablesAndColumns(db *sql.DB, dbName string) (map[string][]string, error) {
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

//GetAllPrimaryKeysInTable returns a map of tables to a list of curresponding primary keys
func GetAllPrimaryKeysInTable(db *sql.DB, tablename string) ([]string, error) {

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

// CreateMetaChangeLogTable : Creates a table to store all changes, old and new, alongside the
// ID (Primary Key) | TableName | PrimaryKeys (x:xval,y:yval) | OldColumnValues (x:xval,y:yval) | NewColumnValue(x:xval,y:yval)  | TriggerType ( I|U|D ) | ActionDate
func CreateMetaChangeLogTable(db *sql.DB) error {

	query := fmt.Sprintf(`CREATE TABLE meta_changelog(
		 ID int NOT NULL AUTO_INCREMENT,
		 TableName varchar(255),
		 PrimaryKeys varchar(255),
		 OldColumnValue varchar(255),
		 NewColumnValue varchar(255),
		 TriggerType varchar(10),
		 ActionDate datetime NOT NULL DEFAULT NOW(),
		 PRIMARY KEY(ID)
	);`)

	_, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
