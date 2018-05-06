package main

import (
	"github.com/thomas-bamilo/operationsellerscoring/connectdb"
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/createvalidation"
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/validationtogsheet"
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
	omsIDSupplierTable := createvalidation.QueryIDSupplierTable(omsDb)
	inboundIssueIDSupplierGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1001607611)
	validationtogsheet.IDSupplierValidationToGsheet(omsIDSupplierTable, inboundIssueIDSupplierGsheet)
	sellerRejectionIDSupplierGsheet := validationtogsheet.GetGsheet("12zINw_v3OSirIDjKGheU07G8kBfNgWStG8kVzHvRD6U", 1860332800)
	validationtogsheet.IDSupplierValidationToGsheet(omsIDSupplierTable, sellerRejectionIDSupplierGsheet)

	inboundIssueResponseGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 199289760)
	baaDb := connectdb.ConnectToBaa()
	defer baaDb.Close()
	createvalidation.EmailToBaa(inboundIssueResponseGsheet, baaDb)
	emailTable := createvalidation.BaaToEmailTable(baaDb)
	inboundIssueIDEmailGsheet := validationtogsheet.GetGsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU", 1898441539)
	validationtogsheet.EmailValidationToGsheet(emailTable, inboundIssueIDEmailGsheet)

}
