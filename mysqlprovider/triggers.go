//Package mysqlprovider package exports database access queries, to help decrease clutter in the packages using these queries.
package mysqlprovider

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

func createTriggers(db *sql.DB, dbName, TriggerChangelogTable string) error {
	tablesAndColumns, err := getAllTablesAndColumns(db, dbName)
	if err != nil {
		log.Println(err)
		return err
	}

	//loop through all tables to set triggers for each of them.
	for tablename, columns := range tablesAndColumns {
		if tablename == TriggerChangelogTable {
			continue
		}

		var OldPrimaryKeysAndValues, NewPrimaryKeysAndValues string

		primaryKeysList, err := getAllPrimaryKeysInTable(db, tablename)
		if err != nil {
			log.Println(err)
		}

		for _, pks := range primaryKeysList {
			OldPrimaryKeysAndValues += "'" + pks + ":',OLD." + pks + ",',',"
			NewPrimaryKeysAndValues += "'" + pks + ":',NEW." + pks + ",',',"
		}
		OldPrimaryKeysAndValues += "''"
		NewPrimaryKeysAndValues += "''"

		var OldColumnValues string
		var NewColumnValues string

		for _, columnName := range columns {
			OldColumnValues += "'" + columnName + ":',OLD." + columnName + ",',',"
			NewColumnValues += "'" + columnName + ":',NEW." + columnName + ",',',"
		}

		OldColumnValues += "''"
		NewColumnValues += "''"

		err = createInsertTrigger(db, tablename, TriggerChangelogTable, NewPrimaryKeysAndValues, NewColumnValues)
		if err != nil {
			log.Println(err)
		}

		err = createUpdateTrigger(db, tablename, TriggerChangelogTable, NewPrimaryKeysAndValues, OldColumnValues, NewColumnValues)
		if err != nil {
			log.Println(err)
		}
		err = createDeleteTrigger(db, tablename, TriggerChangelogTable, OldPrimaryKeysAndValues, OldColumnValues)
		if err != nil {
			log.Println(err)
		}

	}
	return nil
}

//createInsertTrigger for any given tablename, to store any newly added value to the table, and a form of primary key to access the data if need be.
func createInsertTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_insert_trigger;`, tablename)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
    CREATE TRIGGER %[1]s_insert_trigger
      AFTER INSERT ON %[1]s
      FOR EACH ROW
        INSERT INTO %[2]s (TableName, PrimaryKeys, NewColumnValue,  TriggerType )
        VALUES ('%[1]s',CONCAT(%[3]s),CONCAT(%[4]s),'I');
    `, tablename, TriggerChangelogTable, primaryKeysAndValues, NewColumnValues)

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//createUpdateTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be, and also the updated data to another column.
func createUpdateTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
			DROP TRIGGER %s_update_trigger;`, tablename)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
			CREATE TRIGGER %[1]s_update_trigger
		    AFTER UPDATE ON %[1]s
		    FOR EACH ROW
		      INSERT INTO %[2]s (TableName, PrimaryKeys, OldColumnValue, NewColumnValue, TriggerType )
			    VALUES ('%[1]s',CONCAT(%[3]s),CONCAT(%[4]s),CONCAT(%[5]s), 'U');
      `, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues, NewColumnValues)

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//CreateDeleteTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be.
func createDeleteTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_delete_trigger;`, tablename)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
    CREATE TRIGGER %[1]s_delete_trigger
      AFTER DELETE ON %[1]s
      FOR EACH ROW
        INSERT INTO %[2]s (TableName, PrimaryKeys, OldColumnValue,  TriggerType )
        VALUES ('%[1]s',CONCAT(%[3]s),CONCAT(%[4]s),'D');
    `, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues)

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
