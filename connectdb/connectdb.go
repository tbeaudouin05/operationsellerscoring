package connectdb

import (
	"database/sql"
	"log"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thomas-bamilo/dbconf"

	// driver for sqlite3
	_ "github.com/mattn/go-sqlite3"
)

// ConnectToOms returns a MySQL database connection to oms_live_ir
func ConnectToOms() *sql.DB {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := dbConf.OmsUser + ":" + dbConf.OmsPw + "@tcp(" + dbConf.OmsHost + ")/" + dbConf.OmsDb

	// connect to database
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}

// ConnectToBaa returns a SQL Server database connection to baa_application
func ConnectToBaa() *sql.DB {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := `sqlserver://` + dbConf.BaaUser + ":" + dbConf.BaaPw + "@" + dbConf.BaaHost + "/" + dbConf.BaaDb

	// connect to database
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}

// ConnectToBi returns a SQL Server database connection to bi_replica database
func ConnectToBi() *sql.DB {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := `sqlserver://` + dbConf.BiUser + ":" + dbConf.BiPw + "@" + dbConf.BiHost + "/" + dbConf.BiDb

	// connect to database
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}

// ConnectToSQLite returns a SQLite3 in-memory database connection
func ConnectToSQLite() *sql.DB {
	// create database in shared memory (in memory but different queries can access it because it is cached and shared)
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	return db
}
