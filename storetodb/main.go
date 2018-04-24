package main

import (
	"log"
	"os"

	"github.com/thomas-bamilo/operationsellerscoring/sellerdisciplinerow"
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

	log.Println("Fetching data from Seller Discipline response sheet")
	sellerDisciplineTable := gsheetinteract.CreateSellerDisciplineTable(sellerDisciplineResponseSheet)

	log.Println("Filter response sheet for supplier_id is not null and wrong supplier names")

	sellerDisciplineTableValidRow, sellerDisciplineTableInvalidRow := sellerdisciplinerow.FilterSellerDisciplineTable(sellerDisciplineTable)

	sellerDisciplineInvalidRowSheet := gsheetinteract.FetchGsheetByID("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1015531072)

	log.Println("Update invalid row sheet Google sheet")
	gsheetinteract.UpdateInvalidRowSheet(sellerDisciplineInvalidRowSheet, sellerDisciplineTableInvalidRow)

	log.Println("Load sellerDisciplineTableValidRow to BAA database")
	baadbinteract.LoadSellerDisciplineTableValidRowToBaaDb(sellerDisciplineTableValidRow)

}
