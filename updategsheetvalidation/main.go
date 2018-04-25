package main

import (
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/createvalidation"
	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/validationtogsheet"
)

func main() {

	omsDb := createvalidation.ConnectToOms()

	//bobDb := createvalidation.ConnectToBob()

	defer omsDb.Close()
	//defer bobDb.Close()

	inboundIssueSpreadsheet := validationtogsheet.GetSpreadsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU")

	validationtogsheet.IDSupplierValidationToGsheet(omsDb, inboundIssueSpreadsheet)

	//validationtogsheet.BrandValidationToGsheet(bobDb, inboundIssueSpreadsheet)

}
