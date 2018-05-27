package main

import (
	"github.com/thomas-bamilo/operation/operationsellerscoring/updategsheetvalidation/dbinteract"
	"github.com/thomas-bamilo/operation/operationsellerscoring/updategsheetvalidation/validationtogsheet"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	/*// used for logging
	f, err := os.OpenFile("logfile.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)*/

	omsDb := connectdb.ConnectToOms()
	defer omsDb.Close()
	omsIDSupplierTable := dbinteract.QueryIDSupplierTable(omsDb)
	inboundIssueIDSupplierGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1001607611)
	validationtogsheet.IDSupplierValidationToGsheet(omsIDSupplierTable, inboundIssueIDSupplierGsheet)
	sellerRejectionIDSupplierGsheet := validationtogsheet.GetGsheet("12zINw_v3OSirIDjKGheU07G8kBfNgWStG8kVzHvRD6U", 1860332800)
	validationtogsheet.IDSupplierValidationToGsheet(omsIDSupplierTable, sellerRejectionIDSupplierGsheet)

	inboundIssueResponseGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 199289760)
	baaDb := connectdb.ConnectToBaa()
	defer baaDb.Close()
	dbinteract.EmailToBaa(inboundIssueResponseGsheet, baaDb)
	emailTable := dbinteract.BaaToEmailTable(baaDb)
	inboundIssueIDEmailGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1898441539)
	validationtogsheet.EmailValidationToGsheet(emailTable, inboundIssueIDEmailGsheet)

}
