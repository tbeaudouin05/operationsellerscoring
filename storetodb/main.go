package main

import (
	"log"
	"os"

	"github.com/thomas-bamilo/operationsellerscoring/connectdb"
	"github.com/thomas-bamilo/operationsellerscoring/inboundissuerow"
	"github.com/thomas-bamilo/operationsellerscoring/storetodb/baadbinteract"
	"github.com/thomas-bamilo/operationsellerscoring/storetodb/gsheetinteract"
)

func main() {

	// used for logging
	f, err := os.OpenFile("logfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	sellerDisciplineResponseSheet := gsheetinteract.FetchGsheetByID("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 199289760)

	log.Println("Fetching data from Inbound Issue response sheet")
	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	InboundIssueTable := gsheetinteract.CreateInboundIssueTable(dbBaa, sellerDisciplineResponseSheet)

	log.Println("Filter response sheet for valid vs. invalid rows")
	InboundIssueTableValidRow, InboundIssueTableInvalidRow := inboundissuerow.FilterInboundIssueTable(InboundIssueTable)

	sellerDisciplineInvalidRowSheet := gsheetinteract.FetchGsheetByID("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1015531072)

	log.Println("Update invalid row Google sheet")
	gsheetinteract.UpdateInvalidRowSheet(sellerDisciplineInvalidRowSheet, InboundIssueTableInvalidRow)

	log.Println("Load InboundIssueTableValidRow to BAA database")
	baadbinteract.LoadInboundIssueTableValidRowToBaaDb(dbBaa, InboundIssueTableValidRow)

}
