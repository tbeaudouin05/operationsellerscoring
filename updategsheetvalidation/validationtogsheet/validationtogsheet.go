package validationtogsheet

import (
	"database/sql"
	"io/ioutil"

	"github.com/thomas-bamilo/operationsellerscoring/updategsheetvalidation/createvalidation"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

// GetSpreadsheet returns a Spreadsheet object representing this Google spreadsheet: https://goo.gl/7FCRvp
func GetSpreadsheet(spreadsheetID string) spreadsheet.Spreadsheet {

	data, err := ioutil.ReadFile("client_secret.json")
	checkError(err)
	conf, err := google.JWTConfigFromJSON(data, spreadsheet.Scope)
	checkError(err)
	client := conf.Client(context.TODO())

	service := spreadsheet.NewServiceWithClient(client)
	spreadsheet, err := service.FetchSpreadsheet(spreadsheetID)
	checkError(err)

	return spreadsheet

}

// IDSupplierValidationToGsheet retrieves omsIDSupplierTable from oms_database and writes it to https://goo.gl/PRgBcy
func IDSupplierValidationToGsheet(db *sql.DB, spreadsheet spreadsheet.Spreadsheet) {

	gsheet, err := spreadsheet.SheetByID(1001607611)
	checkError(err)

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

	// create omsSupplierIDTable
	omsIDSupplierTable := createvalidation.QueryIDSupplierTable(db)

	// update ggsheet with omsIDSupplierTable
	gsheet.Update(0, 0, "supplier_name")
	gsheet.Update(0, 1, "supplier_id")
	for i := 0; i < len(omsIDSupplierTable); i++ {
		gsheet.Update(i+1, 0, omsIDSupplierTable[i].SupplierName)
		gsheet.Update(i+1, 1, omsIDSupplierTable[i].IDSupplier)

	}

	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

}

// BrandValidationToGsheet retrieves bobBrandTable from oms_database and writes it to https://goo.gl/vgntMR
func BrandValidationToGsheet(db *sql.DB, spreadsheet spreadsheet.Spreadsheet) {

	gsheet, err := spreadsheet.SheetByID(1512513973)
	checkError(err)

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

	// create bobBrandTable
	bobBrandTable := createvalidation.QueryBrandTable(db)

	// update ggsheet with bobBrandTable
	gsheet.Update(0, 0, "brand")
	for i := 0; i < len(bobBrandTable); i++ {
		gsheet.Update(i+1, 0, bobBrandTable[i].Brand)

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
