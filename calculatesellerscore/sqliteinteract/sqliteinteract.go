package sqliteinteract

import (
	"database/sql"
	"log"
	"time"

	"github.com/thomas-bamilo/operationsellerscoring/storetodb/gsheetinteract"

	// driver for sqlite3
	_ "github.com/mattn/go-sqlite3"
)

// JoinScOmsToCsv joins seller_penalty and sc_item_id tables on oms_item_number and write result to csv file in the same folder as the application
func LoadSellerDisciplineTableToDb([]gsheetinteract.SellerDisciplineRow) {

	// create database in shared memory (in memory but different queries can access it because it is cached and shared)
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

	// SHOULD HAVE DROP TABLE IF EXISTS
	//DROP TABLE IF EXISTS dbo.Scores
	// create seller_discipline table
	createSellerDisciplineTableStr := `CREATE TABLE seller_discipline (
		timestamp STRING
		,item_issue_inbound_failed_reason STRING
		,supplier_id INTEGER`

	createSellerPenaltyTable, err := db.Prepare(createSellerPenaltyTableStr)
	if err != nil {
		log.Fatal(err)
	}
	createSellerPenaltyTable.Exec()

	// insert values into seller_discipline table
	insertSellerPenaltyTableStr := `INSERT INTO seller_discipline (
		supplier_name
		,order_nr
		,bob_item_number
		,oms_item_number
		,return_reason
		,cancel_reason
		,year_month
		,amount) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	insertSellerPenaltyTable, err := db.Prepare(insertSellerPenaltyTableStr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(sellerPenalty); i++ {
		insertSellerPenaltyTable.Exec(sellerPenalty[i].SupplierName,
			sellerPenalty[i].OrderNr,
			sellerPenalty[i].BobItemNumber,
			sellerPenalty[i].OmsItemNumber,
			sellerPenalty[i].ReturnReason,
			sellerPenalty[i].CancelReason,
			sellerPenalty[i].YearMonth,
			sellerPenalty[i].Amount,
		)
		time.Sleep(1 * time.Millisecond)
	}

	/* join seller_penalty and sc_item_id table
		query := `SELECT
		sp.supplier_name
		,sp.order_nr
		,sii.sc_item_number
		,sp.bob_item_number
		,sp.oms_item_number
		,sp.return_reason
		,sp.cancel_reason
		,sp.year_month
		,sp.amount
	   FROM seller_penalty sp
	   JOIN sc_item_id sii
	   ON sp.oms_item_number = sii.oms_item_number`

		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}*/

}

func changeDateType(dateTimeStrIn string) string {

	dateTimeParsed, e := time.Parse(dateTimeStrIn, "1/2/2006 15:04:05")
	return string(dateTimeParsed.Format("2006-01-02 15:04:05"))

}
