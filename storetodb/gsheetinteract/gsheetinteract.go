package gsheetinteract

import (
	"database/sql"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/thomas-bamilo/operationsellerscoring/inboundissuerow"
	"github.com/thomas-bamilo/operationsellerscoring/sellerrejectionrow"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"gopkg.in/Iwark/spreadsheet.v2"

	"github.com/thomas-bamilo/operationsellerscoring/storetodb/baadbinteract"
)

// CreateInboundIssueTable fetches data from a gsheet to create an array of InboundIssueRow which represents inboundIssueTable, the table which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "fk_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
// - uses functions baadbinteract.GetIDInboundIssueFromBaa and newInboundIssue to return only the rows which are not already in baa database
func CreateInboundIssueTable(db *sql.DB, gsheet *spreadsheet.Sheet) []inboundissuerow.InboundIssueRow {

	var inboundIssueTable []inboundissuerow.InboundIssueRow

	for _, row := range gsheet.Rows[1:] {

		StartTimeTroubleshootP, _ := time.Parse(row[14].Value, "1/2/2006 15:04:05")
		EndTimeTroubleshootP, _ := time.Parse(row[15].Value, "1/2/2006 15:04:05")
		NumberOfItemInt, _ := strconv.Atoi(row[16].Value)

		inboundIssueTable = append(inboundIssueTable,
			inboundissuerow.InboundIssueRow{
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
				FKSupplier:            row[17].Value,
				IDInboundIssue:        row[18].Value,
				FKEmail:               row[19].Value,
				DurationTroubleshoot:  int(EndTimeTroubleshootP.Sub(StartTimeTroubleshootP)),
			})
	}

	oldInboundIssueTable := baadbinteract.GetIDInboundIssueFromBaa(db)

	newInboundIssueTable := newInboundIssue(oldInboundIssueTable, inboundIssueTable)

	return newInboundIssueTable
}

// CreateSellerRejectionTable fetches data from a gsheet to create an array of SellerRejectionRow which represents sellerRejectionTable, the table which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "fk_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
// - uses functions baadbinteract.GetIDInboundIssueFromBaa and newSellerRejection to return only the rows which are not already in baa database
func CreateSellerRejectionTable(db *sql.DB, gsheet *spreadsheet.Sheet) []sellerrejectionrow.SellerRejectionRow {

	var sellerRejectionTable []sellerrejectionrow.SellerRejectionRow

	for _, row := range gsheet.Rows[1:] {

		sellerRejectionTable = append(sellerRejectionTable,
			sellerrejectionrow.SellerRejectionRow{
				Timestamp:                   row[0].Value,
				ItemUID:                     eraseAllSpace(strings.ToUpper(row[1].Value[:3]) + row[1].Value[4:]),
				RsReturnOrderNumber:         strings.ToUpper(eraseAllSpace(row[2].Value)),
				ShippingToSellerDate:        row[3].Value,
				RfcReturnFromCustomerReason: row[4].Value,

				RtsSellerRejectionReason:     row[5].Value,
				RtsSellerRejectionDesc:       row[6].Value,
				ItemUnitPrice:                row[7].Value,
				SupplierName:                 row[8].Value,
				CustomerOrderNumber:          row[9].Value,
				LocationSection:              row[10].Value,
				SellerRejectionApprovedYesNo: row[11].Value,
				ApprovalRejectionDesc:        row[12].Value,
				FKSupplier:                   row[13].Value,
				IDSellerRejection:            row[14].Value,
			})
	}

	oldSellerRejectionTable := baadbinteract.GetIDSellerRejectionFromBaa(db)

	newSellerRejectionTable := newSellerRejection(oldSellerRejectionTable, sellerRejectionTable)

	return newSellerRejectionTable
}

func newInboundIssue(oldInboundIssueTable, inboundIssueTable []inboundissuerow.InboundIssueRow) (newInboundIssueTable []inboundissuerow.InboundIssueRow) {

	// initialize oldInboundIssueMap with oldInboundIssueRow
	oldInboundIssueMap := make(map[string]bool)
	for _, oldInboundIssueRow := range oldInboundIssueTable {
		oldInboundIssueMap[oldInboundIssueRow.IDInboundIssue] = true
	}

	// check if any EmailRow from inboundIssueTable is not in oldInboundIssueMap - if not, add the EmailRow to newInboundIssueTable
	for _, oldInboundIssueRow := range inboundIssueTable {
		if _, ok := oldInboundIssueMap[oldInboundIssueRow.IDInboundIssue]; !ok {
			newInboundIssueTable = append(newInboundIssueTable, oldInboundIssueRow)
		}

	}
	return newInboundIssueTable

}

func newSellerRejection(oldSellerRejectionTable, sellerRejectionTable []sellerrejectionrow.SellerRejectionRow) (newSellerRejectionTable []sellerrejectionrow.SellerRejectionRow) {

	// initialize oldInboundIssueMap with oldSellerRejectionRow
	oldInboundIssueMap := make(map[string]bool)
	for _, oldSellerRejectionRow := range oldSellerRejectionTable {
		oldInboundIssueMap[oldSellerRejectionRow.IDSellerRejection] = true
	}

	// check if any EmailRow from sellerRejectionTable is not in oldInboundIssueMap - if not, add the EmailRow to newSellerRejectionTable
	for _, oldSellerRejectionRow := range sellerRejectionTable {
		if _, ok := oldInboundIssueMap[oldSellerRejectionRow.IDSellerRejection]; !ok {
			newSellerRejectionTable = append(newSellerRejectionTable, oldSellerRejectionRow)
		}

	}
	return newSellerRejectionTable

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

// UpdateInboundIssueInvalidRowSheet writes IDInboundIssue and Err column of inboundIssueTableInvalidRow into a gsheet
func UpdateInboundIssueInvalidRowSheet(gsheet *spreadsheet.Sheet, inboundIssueTableInvalidRow []inboundissuerow.InboundIssueRow) {

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err := gsheet.Synchronize()
	checkError(err)

	// update gsheet with inboundIssueTableWrongSupplierName
	gsheet.Update(0, 0, "id_inbound_issue")
	gsheet.Update(0, 1, "error")

	for i := 0; i < len(inboundIssueTableInvalidRow); i++ {
		gsheet.Update(i+1, 0, inboundIssueTableInvalidRow[i].IDInboundIssue)
		gsheet.Update(i+1, 1, inboundIssueTableInvalidRow[i].Err)
	}

	// Make sure call Synchronize to reflect the changes
	err = gsheet.Synchronize()
	checkError(err)

}

// UpdateSellerRejectionInvalidRowSheet writes IDSellerRejection and Err column of sellerRejectionTableInvalidRow into a gsheet
func UpdateSellerRejectionInvalidRowSheet(gsheet *spreadsheet.Sheet, sellerRejectionTableInvalidRow []sellerrejectionrow.SellerRejectionRow) {

	// erase all previous data from gsheet CAREFUL!
	for _, row := range gsheet.Rows {
		for _, cell := range row {
			gsheet.Update(int(cell.Row), int(cell.Column), "")
		}
	}
	// Make sure call Synchronize to reflect the changes
	err := gsheet.Synchronize()
	checkError(err)

	// update gsheet with sellerRejectionTableWrongSupplierName
	gsheet.Update(0, 0, "id_seller_rejection")
	gsheet.Update(0, 1, "error")

	for i := 0; i < len(sellerRejectionTableInvalidRow); i++ {
		gsheet.Update(i+1, 0, sellerRejectionTableInvalidRow[i].IDSellerRejection)
		gsheet.Update(i+1, 1, sellerRejectionTableInvalidRow[i].Err)
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
