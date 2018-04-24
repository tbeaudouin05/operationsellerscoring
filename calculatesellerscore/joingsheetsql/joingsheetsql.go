package joingsheetsql

import (
	"database/sql"
	"log"
	"time"

	"github.com/thomas-bamilo/vs_penalty_automation/scitemid"
	sellerpenalty "github.com/thomas-bamilo/vs_penalty_automation/sellerpenalty"

	"github.com/joho/sqltocsv"
	// driver for sqlite3
	_ "github.com/mattn/go-sqlite3"
)

// JoinScOmsToCsv joins seller_penalty and sc_item_id tables on oms_item_number and write result to csv file in the same folder as the application
func JoinScOmsToCsv(sellerPenalty []sellerpenalty.SellerPenalty, scItemID []scitemid.ScItemID) {

	// create database in shared memory (in memory but different queries can access it because it is cached and shared)
	database, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// create seller_penalty table
	createSellerPenaltyTableStr := `CREATE TABLE IF NOT EXISTS seller_penalty (
		supplier_name TEXT
		,order_nr INTEGER
		,bob_item_number INTEGER
		,oms_item_number INTEGER PRIMARY KEY
		,return_reason TEXT
		,cancel_reason TEXT
		,year_month INTEGER
		,amount INTEGER)`
	createSellerPenaltyTable, err := database.Prepare(createSellerPenaltyTableStr)
	if err != nil {
		log.Fatal(err)
	}
	createSellerPenaltyTable.Exec()

	// create sc_item_id table
	createScItemIDTableStr := `CREATE TABLE IF NOT EXISTS sc_item_id (
		sc_item_number INTEGER
		,oms_item_number INTEGER PRIMARY KEY)`
	createScItemIDTable, err := database.Prepare(createScItemIDTableStr)
	if err != nil {
		log.Fatal(err)
	}
	createScItemIDTable.Exec()

	// insert values into seller_penalty table
	insertSellerPenaltyTableStr := `INSERT INTO seller_penalty (
		supplier_name
		,order_nr
		,bob_item_number
		,oms_item_number
		,return_reason
		,cancel_reason
		,year_month
		,amount) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	insertSellerPenaltyTable, err := database.Prepare(insertSellerPenaltyTableStr)
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

	// insert values into sc_item_id table
	insertScItemIDTableStr := `INSERT INTO sc_item_id (sc_item_number, oms_item_number) VALUES (?, ?)`
	insertScItemIDTable, err := database.Prepare(insertScItemIDTableStr)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < len(scItemID); i++ {
		insertScItemIDTable.Exec(scItemID[i].ScItemNumber, scItemID[i].OmsItemNumber)
		time.Sleep(1 * time.Millisecond)
	}

	// join seller_penalty and sc_item_id table
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

	rows, err := database.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	err = sqltocsv.WriteFile("seller_penalty.csv", rows)
	if err != nil {
		log.Fatal(err)
	}

}
