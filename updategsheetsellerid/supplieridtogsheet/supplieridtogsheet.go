package supplieridtogsheet

import (
	"io/ioutil"

	"github.com/thomas-bamilo/operationsellerscoring/updategsheetsellerid/omssupplierid"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

// SupplierIDToGsheet retrieves omsSupplierIDTable from oms_database and writes it to https://goo.gl/PRgBcy
func SupplierIDToGsheet() {

	data, err := ioutil.ReadFile("client_secret.json")
	checkError(err)
	conf, err := google.JWTConfigFromJSON(data, spreadsheet.Scope)
	checkError(err)
	client := conf.Client(context.TODO())

	service := spreadsheet.NewServiceWithClient(client)
	spreadsheet, err := service.FetchSpreadsheet("1wDTaZVLmos6-B79626H1531_JMgo1b5nBDKJP7NwsPU")
	checkError(err)
	sheet, err := spreadsheet.SheetByID(1001607611)
	checkError(err)

	// erase all previous data from gsheet CAREFUL!
	for _, row := range sheet.Rows {
		for _, cell := range row {
			sheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err = sheet.Synchronize()
	checkError(err)

	// create omsSupplierIDTable
	omsSupplierIDTable := omssupplierid.CreateSupplierIDTable()

	// update gsheet with omsSupplierIDTable
	sheet.Update(0, 0, "supplier_name")
	sheet.Update(0, 1, "supplier_id")
	for i := 0; i < len(omsSupplierIDTable); i++ {
		sheet.Update(i+1, 0, omsSupplierIDTable[i].SupplierName)
		sheet.Update(i+1, 1, omsSupplierIDTable[i].SupplierID)

	}

	// Make sure call Synchronize to reflect the changes
	err = sheet.Synchronize()
	checkError(err)

}

func checkError(err error) {
	if err != nil {
		panic(err.Error())
	}
}
