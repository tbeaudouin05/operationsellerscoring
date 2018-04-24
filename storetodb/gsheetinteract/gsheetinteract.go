package gsheetinteract

import (
	"io/ioutil"

	"github.com/thomas-bamilo/operationsellerscoring/sellerdisciplinerow"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"
)

// CreateSellerDisciplineTable fetches data from a gsheet to create an array of SellerDisciplineRow which represents sellerDisciplineTable, the table which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "id_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
func CreateSellerDisciplineTable(gsheet *spreadsheet.Sheet) []sellerdisciplinerow.SellerDisciplineRow {

	var sellerDisciplineTable []sellerdisciplinerow.SellerDisciplineRow

	for _, row := range gsheet.Rows[1:] {

		sellerDisciplineTable = append(sellerDisciplineTable,
			sellerdisciplinerow.SellerDisciplineRow{
				Timestamp:                    row[0].Value,
				PoNumber:                     row[2].Value,
				OrderNumber:                  row[3].Value,
				ItemIssueInboundFailedReason: row[4].Value,
				OrderCancelledYesNo:          row[5].Value,
				Comment:                      row[6].Value,
				EmailAddress:                 row[7].Value,
				OriginalSellerFoundYesNo:     row[8].Value,
				SupplierName:                 row[9].Value,
				Category:                     row[10].Value,
				Brand:                        row[11].Value,
				Description:                  row[12].Value,
				Sku:                          row[13].Value,
				StartTimeTroubleshoot: row[14].Value,
				EndTimeTroubleshoot:   row[15].Value,
				IDSupplier:            row[17].Value,
				IDInboundIssue:        row[18].Value,
			})
	}

	return sellerDisciplineTable
}

// FetchGsheetByID fetches the gsheet of a google spreadsheet given its spreadsheetID and sheetID
func FetchGsheetByID(spreadsheetID string, sheetID uint) *spreadsheet.Sheet {
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

// UpdateInvalidRowSheet writes sellerDisciplineTableInvalidRow incl. Err column into a gsheet
func UpdateInvalidRowSheet(gsheet *spreadsheet.Sheet, sellerDisciplineTableInvalidRow []sellerdisciplinerow.SellerDisciplineRow) {

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err := gsheet.Synchronize()
	checkError(err)

	// update gsheet with sellerDisciplineTableWrongSupplierName
	gsheet.Update(0, 0, "id_inbound_issue")
	gsheet.Update(0, 1, "error")

	for i := 0; i < len(sellerDisciplineTableInvalidRow); i++ {
		gsheet.Update(i+1, 0, sellerDisciplineTableInvalidRow[i].IDInboundIssue)
		gsheet.Update(i+1, 1, sellerDisciplineTableInvalidRow[i].Err)
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