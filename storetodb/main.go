package main

import (
	"log"
	"os"

	"github.com/thomas-bamilo/operationsellerscoring/connectdb"
	"github.com/thomas-bamilo/operationsellerscoring/inboundissuerow"
	"github.com/thomas-bamilo/operationsellerscoring/sellerrejectionrow"
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

	log.Println("Connecting to necessary gsheets")
	inboundIssueResponseSheet := gsheetinteract.FetchGsheetByID("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 199289760)
	inboundIssueInvalidRowSheet := gsheetinteract.FetchGsheetByID("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1015531072)
	sellerRejectionResponseSheet := gsheetinteract.FetchGsheetByID("12zINw_v3OSirIDjKGheU07G8kBfNgWStG8kVzHvRD6U", 1333014143)
	sellerRejectionInvalidRowSheet := gsheetinteract.FetchGsheetByID("12zINw_v3OSirIDjKGheU07G8kBfNgWStG8kVzHvRD6U", 1116714321)

	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	log.Println("Fetching data from Inbound Issue response sheet")
	InboundIssueTable := gsheetinteract.CreateInboundIssueTable(dbBaa, inboundIssueResponseSheet)
	log.Println("Fetching data from Seller Rejection response sheet")
	SellerRejectionTable := gsheetinteract.CreateSellerRejectionTable(dbBaa, sellerRejectionResponseSheet)

	log.Println("Filter response sheets for valid vs. invalid rows")
	InboundIssueTableValidRow, InboundIssueTableInvalidRow := inboundissuerow.FilterInboundIssueTable(InboundIssueTable)
	SellerRejectionTableValidRow, SellerRejectionTableInvalidRow := sellerrejectionrow.FilterSellerRejectionTable(SellerRejectionTable)

	log.Println("Update invalid row Gsheets")
	gsheetinteract.UpdateInboundIssueInvalidRowSheet(inboundIssueInvalidRowSheet, InboundIssueTableInvalidRow)
	gsheetinteract.UpdateSellerRejectionInvalidRowSheet(sellerRejectionInvalidRowSheet, SellerRejectionTableInvalidRow)

	log.Println("Load InboundIssueTableValidRow and SellerRejectionTableValidRow to BAA database")
	baadbinteract.LoadInboundIssueTableValidRowToBaaDb(dbBaa, InboundIssueTableValidRow)
	baadbinteract.LoadSellerRejectionTableValidRowToBaaDb(dbBaa, SellerRejectionTableValidRow)

}
