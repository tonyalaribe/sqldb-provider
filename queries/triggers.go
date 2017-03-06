//Package queries package exports database access queries, to help decrease clutter in the packages using these queries.
package queries

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql" //A mysql driver to allow database/sql understand the database
)

//CreateInsertTrigger for any given tablename, to store any newly added value to the table, and a form of primary key to access the data if need be.
func CreateInsertTrigger(db *sql.DB, tablename, primaryKeysAndValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_insert_trigger;`, tablename)

	_, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
    CREATE TRIGGER %s_insert_trigger
      AFTER DELETE ON %s
      FOR EACH ROW
        INSERT INTO meta_changelog (TableName, PrimaryKeys, NewColumnValue,  TriggerType )
        VALUES ('%s',CONCAT(%s),CONCAT(%s),'D');
    `, tablename, tablename, tablename, primaryKeysAndValues, NewColumnValues)

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//CreateUpdateTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be, and also the updated data to another column.
func CreateUpdateTrigger(db *sql.DB, tablename, primaryKeysAndValues, OldColumnValues, NewColumnValues string) error {
	query := fmt.Sprintf(`
			DROP TRIGGER %s_update_trigger;`, tablename)

	_, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
			CREATE TRIGGER %s_update_trigger
		    AFTER UPDATE ON %s
		    FOR EACH ROW
		      INSERT INTO meta_changelog (TableName, PrimaryKeys, OldColumnValue, NewColumnValue, TriggerType )
			    VALUES ('%s',CONCAT(%s),CONCAT(%s),CONCAT(%s), 'U');
      `, tablename, tablename, tablename, primaryKeysAndValues, OldColumnValues, NewColumnValues)

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

//CreateDeleteTrigger for any given tablename, to store its previous values in the db, and a form of primary key to access the data if need be.
func CreateDeleteTrigger(db *sql.DB, tablename, primaryKeysAndValues, OldColumnValues string) error {
	query := fmt.Sprintf(`
    DROP TRIGGER %s_delete_trigger;`, tablename)

	_, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	query = fmt.Sprintf(`
    CREATE TRIGGER %s_delete_trigger
      AFTER DELETE ON %s
      FOR EACH ROW
        INSERT INTO meta_changelog (TableName, PrimaryKeys, OldColumnValue,  TriggerType )
        VALUES ('%s',CONCAT(%s),CONCAT(%s),'D');
    `, tablename, tablename, tablename, primaryKeysAndValues, OldColumnValues)

	_, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
