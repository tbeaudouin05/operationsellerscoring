package main

import (
	"log"

	"github.com/thomas-bamilo/email/goemail"
	"github.com/thomas-bamilo/operationsellerscoring/calculatesellerscore/bidbinteract"
	"github.com/thomas-bamilo/operationsellerscoring/calculatesellerscore/sqliteinteract"
	"github.com/thomas-bamilo/operationsellerscoring/storetodb/baadbinteract"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	log.Println("Creating inbound_score table")
	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	inboundScoreTable := baadbinteract.InboundScoreTableFromBaa(dbBaa)

	log.Println("Creating rts table")
	rtsTable := baadbinteract.RtsTableFromBaa(dbBaa)

	log.Println("Creating ttr_rfc table")
	dbBi := connectdb.ConnectToBi()
	defer dbBi.Close()
	ttrRfcTable := bidbinteract.CreateTtrRfcTable(dbBi)

	log.Println("Creating supplier_class table")
	supplierClassTable := bidbinteract.CreateSupplierClassTable(dbBi)

	log.Println("Joining tables")
	dbSQLite := connectdb.ConnectToSQLite()
	defer dbSQLite.Close()
	sqliteinteract.CreateTtrRfcInboundScoreRtsTable(dbSQLite, ttrRfcTable, inboundScoreTable, rtsTable, supplierClassTable)

	goemail.GoEmail()

}
