package main

import (
	"log"
	"os"

	"github.com/thomas-bamilo/operationsellerscoring/connectdb"
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/createvalidation"
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/validationtogsheet"
)

func main() {

	// used for logging
	f, err := os.OpenFile("logfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	inboundIssueSpreadsheet := validationtogsheet.GetSpreadsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU")

	omsDb := connectdb.ConnectToOms()
	defer omsDb.Close()
	validationtogsheet.IDSupplierValidationToGsheet(omsDb, inboundIssueSpreadsheet)

	baaDb := connectdb.ConnectToBaa()
	defer baaDb.Close()

	createvalidation.EmailToBaa(inboundIssueSpreadsheet, baaDb)

	emailTable := createvalidation.BaaToEmailTable(baaDb)

	validationtogsheet.EmailValidationToGsheet(emailTable, inboundIssueSpreadsheet)

}
