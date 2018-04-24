package sellerdisciplinerow

import (
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"
)

// SellerDisciplineRow represents a row of the table sellerDisciplineTable which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "id_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
type SellerDisciplineRow struct {
	Timestamp                    string `json:"timestamp"`
	PoNumber                     string `json:"po_number"`
	OrderNumber                  string `json:"order_number"`
	ItemIssueInboundFailedReason string `json:"item_issue_inbound_failed_reason"`
	OrderCancelledYesNo          string `json:"order_cancelled_yes_no"`
	Comment                      string `json:"comment"`
	EmailAddress                 string `json:"email_address"`
	OriginalSellerFoundYesNo     string `json:"original_seller_found_yes_no"`
	SupplierName                 string `json:"supplier_name"`
	Category                     string `json:"category"`
	Brand                        string `json:"brand"`
	Description                  string `json:"description"`
	Sku                          string `json:"sku"`
	StartTimeTroubleshoot        string `json:"beginning_time_troubleshoot_this_item"`
	EndTimeTroubleshoot          string `json:"ending_time_troubleshoot_this_item"`
	IDSupplier                   string `json:"id_supplier"`
	IDInboundIssue               string `json:"id_inbound_issue"`
	Err                          string `json:"error"`
}

func (row SellerDisciplineRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		// Timestamp cannot be empty, and must be a date in format 2006/01/02 15:04:05
		validation.Field(&row.Timestamp, validation.Required, validation.Date("1/2/2006 15:04:05")),
		// IDSupplier cannot be empty, and must be an integer
		validation.Field(&row.IDSupplier, validation.Required, is.Int),
		// EmailAddress cannot be empty, and must be an email
		validation.Field(&row.EmailAddress, validation.Required, is.Email),
		// ItemIssueInboundFailedReason cannot be empty and must be within the list specified
		validation.Field(&row.ItemIssueInboundFailedReason, validation.Required, validation.In(
			"item_issue_inbound_failed_reason",
			"Wrong Item",
			"No Invoice",
			"Other",
			"Defective Item",
			"Defective/Wrong Invoice",
			"Extra Items Sent by Seller",
			"No Packaging",
			"Items Not Sorted",
			"Bad Packaging",
		)),
	)
}

// FilterSellerDisciplineTable splits SellerDisciplineTable into SellerDisciplineTableWSupplierID and SellerDisciplineTableWrongSupplierName.
// NB: rows with IDSupplier = "" & OriginalSellerFoundYesNo = "No" are not considered
func FilterSellerDisciplineTable(sellerDisciplineTable []SellerDisciplineRow) (SellerDisciplineTableValidRow, SellerDisciplineTableInvalidRow []SellerDisciplineRow) {

	// only keep rows with id_supplier and correct data validation
	SellerDisciplineTableValidRow = filterPointer(filter(sellerDisciplineTable, hasSupplierID), isValidRowFormat)

	// only keep rows with id_supplier BUT with incorrect data validation
	SellerDisciplineTableInvalidRowFormat := filterPointer(filter(sellerDisciplineTable, hasSupplierID), isInvalidRowFormat)
	// only keep rows without id_supplier AND with OriginalSellerFoundYesNo = Yes (if No, then we have no way to link data to any supplier, so we exclude these rows)
	SellerDisciplineTableWrongSupplierName := filter(filter(sellerDisciplineTable, hasNoSupplierID), isYesOriginalSellerFound)

	// add error message to SellerDisciplineTableInvalidRowFormat
	for i := 0; i < len(SellerDisciplineTableInvalidRowFormat); i++ {

		SellerDisciplineTableInvalidRowFormat[i].Err = SellerDisciplineTableInvalidRowFormat[i].validateRowFormat().Error()
	}

	// add error message to SellerDisciplineTableWrongSupplierName
	for i := 0; i < len(SellerDisciplineTableWrongSupplierName); i++ {

		SellerDisciplineTableWrongSupplierName[i].Err = "Wrong or missing supplier name!"
	}

	// append SellerDisciplineTableWrongSupplierName to SellerDisciplineTableInvalidRowFormat into SellerDisciplineTableInvalidRow
	SellerDisciplineTableInvalidRow = append(SellerDisciplineTableWrongSupplierName, SellerDisciplineTableInvalidRowFormat...)

	return SellerDisciplineTableValidRow, SellerDisciplineTableInvalidRow

}

func filter(unfilteredTable []SellerDisciplineRow, test func(SellerDisciplineRow) bool) (filteredTable []SellerDisciplineRow) {
	for _, row := range unfilteredTable {
		if test(row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

func filterPointer(unfilteredTable []SellerDisciplineRow, test func(*SellerDisciplineRow) bool) (filteredTable []SellerDisciplineRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

func hasSupplierID(row SellerDisciplineRow) bool {

	return row.IDSupplier != ""

}

func hasNoSupplierID(row SellerDisciplineRow) bool {

	return row.IDSupplier == ""

}

func isYesOriginalSellerFound(row SellerDisciplineRow) bool {

	return row.OriginalSellerFoundYesNo == "Yes"

}

func isValidRowFormat(row *SellerDisciplineRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

func isInvalidRowFormat(row *SellerDisciplineRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}
