package gsheetinteract

import (
	"io/ioutil"
	"strconv"
	"strings"
	"time"
	"unicode"

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

		StartTimeTroubleshootP, _ := time.Parse(row[14].Value, "1/2/2006 15:04:05")
		EndTimeTroubleshootP, _ := time.Parse(row[15].Value, "1/2/2006 15:04:05")
		NumberOfItemInt, _ := strconv.Atoi(row[16].Value)

		sellerDisciplineTable = append(sellerDisciplineTable,
			sellerdisciplinerow.SellerDisciplineRow{
				Timestamp:                    row[0].Value,
				PoNumber:                     strings.ToUpper(eraseAllSpace(row[2].Value)),
				OrderNumber:                  eraseAllSpace(row[3].Value),
				ItemIssueInboundFailedReason: row[4].Value,
				OrderCancelledYesNo:          row[5].Value,
				EmailAddress:                 row[7].Value,
				OriginalSellerFoundYesNo:     row[8].Value,
				SupplierName:                 row[9].Value,
				CategoryDirty:                row[10].Value,
				BrandDirty:                   row[11].Value,
				Sku:                          strings.ToUpper(eraseAllSpace(row[13].Value)),
				StartTimeTroubleshoot: row[14].Value,
				EndTimeTroubleshoot:   row[15].Value,
				NumberOfItem:          NumberOfItemInt,
				IDSupplier:            row[17].Value,
				IDInboundIssue:        row[18].Value,
				DurationTroubleshoot:  int(EndTimeTroubleshootP.Sub(StartTimeTroubleshootP)),
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

// UpdateInvalidRowSheet writes IDInboundIssue and Err column of sellerDisciplineTableInvalidRow into a gsheet
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

func eraseAllSpace(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func checkError(err error) {
	if err != nil {
		panic(err.Error())
	}
}
