package validationtogsheet

import (
	"io/ioutil"

	"github.com/thomas-bamilo/operation/operationsellerscoring/updategsheetvalidation/createvalidation"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

// GetGsheet returns a gsheet object according to spreadsheetID and sheetID
func GetGsheet(spreadsheetID string, sheetID uint) *spreadsheet.Sheet {

	data, err := ioutil.ReadFile("client_secret.json")
	checkError(err)
	conf, err := google.JWTConfigFromJSON(data, spreadsheet.Scope)
	checkError(err)
	client := conf.Client(context.TODO())

	service := spreadsheet.NewServiceWithClient(client)
	spreadsheet, err := service.FetchSpreadsheet(spreadsheetID)
	checkError(err)

	gsheet, err := spreadsheet.SheetByID(sheetID)
	checkError(err)

	return gsheet

}

// IDSupplierValidationToGsheet retrieves omsIDSupplierTable from oms_database and writes it to spreadsheet>idSheet
func IDSupplierValidationToGsheet(omsIDSupplierTable []createvalidation.IDSupplierRow, gsheet *spreadsheet.Sheet) {

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err := gsheet.Synchronize()
	checkError(err)

	// update ggsheet with omsIDSupplierTable
	gsheet.Update(0, 0, "supplier_name")
	gsheet.Update(0, 1, "id_supplier")
	for i := 0; i < len(omsIDSupplierTable); i++ {
		gsheet.Update(i+1, 0, omsIDSupplierTable[i].SupplierName)
		gsheet.Update(i+1, 1, omsIDSupplierTable[i].IDSupplier)

	}

	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

}

// EmailValidationToGsheet retrieves omsIDSupplierTable from oms_database and writes it to https://goo.gl/PRgBcy
func EmailValidationToGsheet(emailTable []createvalidation.EmailRow, gsheet *spreadsheet.Sheet) {

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err := gsheet.Synchronize()
	checkError(err)

	// update ggsheet with omsIDSupplierTable
	gsheet.Update(0, 0, "email")
	gsheet.Update(0, 1, "id_email")
	for i := 0; i < len(emailTable); i++ {
		gsheet.Update(i+1, 0, emailTable[i].Email)
		gsheet.Update(i+1, 1, emailTable[i].IDEmail)

	}

	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

}

func checkError(err error) {
	if err != nil {
		panic(err.Error())
	}
}
