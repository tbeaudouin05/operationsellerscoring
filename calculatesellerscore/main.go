package main

import (
	"log"
	"os"

	"github.com/thomas-bamilo/operationsellerscoring/calculatesellerscore/biqueryttrrfc"
	"github.com/thomas-bamilo/operationsellerscoring/calculatesellerscore/sqliteinteract"
	"github.com/thomas-bamilo/operationsellerscoring/connectdb"
	"github.com/thomas-bamilo/operationsellerscoring/storetodb/baadbinteract"
)

func main() {

	// used for logging
	f, err := os.OpenFile("logfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	log.Println("Creating inbound_score table")
	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	inboundScoreTable := baadbinteract.InboundScoreTableFromBaa(dbBaa)

	log.Println("Creating ttr_rfc table")
	dbBi := connectdb.ConnectToBi()
	defer dbBi.Close()
	ttrRfcTable := biqueryttrrfc.CreateTtrRfcTable(dbBi)

	log.Println("Joining tables")
	dbSQLite := connectdb.ConnectToSQLite()
	defer dbSQLite.Close()
	sqliteinteract.CreateTtrRfcInboundScoreRtsTable(dbSQLite, ttrRfcTable, inboundScoreTable)

}
