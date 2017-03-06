//Package queries package exports database access queries, to help decrease clutter in the packages using these queries.
package queries

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

//CreateInsertTrigger for any given tablename, to store any newly added value to the table, and a form of primary key to access the data if need be.
func CreateInsertTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_insert_trigger;`, tablename)

	_, err := db.Query(query)
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

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//CreateUpdateTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be, and also the updated data to another column.
func CreateUpdateTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
			DROP TRIGGER %s_update_trigger;`, tablename)

	_, err := db.Query(query)
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

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//CreateDeleteTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be.
func CreateDeleteTrigger(db *sql.DB, tablename, TriggerChangelogTable, primaryKeysAndValues, OldColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_delete_trigger;`, tablename)

	_, err := db.Query(query)
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

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
