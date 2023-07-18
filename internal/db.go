package internal

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

// MyDb db struct
type MyDb struct {
	Db     *sql.DB
	dbType string
}

// NewMyDb parse dsn
func NewMyDb(dsn string, dbType string) *MyDb {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Sprintf("connected to db [%s] failed,err=%s", dsn, err))
	}
	return &MyDb{
		Db:     db,
		dbType: dbType,
	}
}

// GetTableNames table names
func (db *MyDb) GetTableNames() []string {
	rs, err := db.Query("show table status")
	if err != nil {
		panic("show tables failed:" + err.Error())
	}
	defer rs.Close()

	var tables []string
	columns, _ := rs.Columns()
	for rs.Next() {
		var values = make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rs.Scan(valuePtrs...); err != nil {
			panic("show tables failed when scan," + err.Error())
		}
		var valObj = make(map[string]any)
		for i, col := range columns {
			var v any
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			valObj[col] = v
		}
		if valObj["Engine"] != nil {
			tables = append(tables, valObj["Name"].(string))
		}
	}
	return tables
}

// GetTableSchema table schema
func (db *MyDb) GetTableSchema(name string) (res string) {
	rs, err := db.Query(fmt.Sprintf("show create table `%s`", name))
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		var vname, schema string
		if err := rs.Scan(&vname, &schema); err != nil {
			panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
		}

		// 解决括号内默认类型的问题
		schema = replaceType(schema)

		// CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
		// 抹除 默认字符集、排序方式 带来的影响
		defaultCharacter, defaultCollate := db.GetTableDefaultCharacterCollate(name)
		schema = strings.ReplaceAll(schema, " CHARACTER SET "+defaultCharacter, "")
		schema = strings.ReplaceAll(schema, " COLLATE "+defaultCollate, "")

		res = schema
		return
	}
	return
}

func (db *MyDb) GetTableDefaultCharacterCollate(name string) (character string, collate string) {
	sql := `
SELECT CCSA.character_set_name AS charset, CCSA.collation_name AS collation
FROM information_schema.tables AS IST
    JOIN information_schema.collation_character_set_applicability AS CCSA ON IST.table_collation = CCSA.collation_name
WHERE IST.table_schema = database()
  AND IST.table_name = ?
  `
	rs, err := db.Query(sql, name)
	if err != nil {
		log.Println(err)
		return
	}
	defer rs.Close()
	for rs.Next() {
		if err := rs.Scan(&character, &collate); err != nil {
			panic(fmt.Sprintf("get table %s 's schema failed, %s", name, err))
		}
		return
	}
	return
}

var needReplace = map[string]string{
	" tinyint(4) ":   " tinyint ",
	" tinyint(3) ":   " tinyint ",
	" smallint(6) ":  " smallint ",
	" mediumint(9) ": " mediumint ",
	" int(11) ":      " int ",
	" int(10) ":      " int ",
	" int(9) ":       " int ",
	" int(8) ":       " int ",
	" int(7) ":       " int ",
	" int(6) ":       " int ",
	" int(5) ":       " int ",
	" int(4) ":       " int ",
	" int(3) ":       " int ",
	" int(2) ":       " int ",
	" int(1) ":       " int ",
	" bigint(20) ":   " bigint ",
	" USING BTREE":   " ",
}

func replaceType(s string) string {
	for key, value := range needReplace {
		s = strings.ReplaceAll(s, key, value)
	}
	return s
}

// Query execute sql query
func (db *MyDb) Query(query string, args ...any) (*sql.Rows, error) {
	log.Println("[SQL]", "["+db.dbType+"]", query, args)
	return db.Db.Query(query, args...)
}
