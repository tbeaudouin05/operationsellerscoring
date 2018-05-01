package inboundissuerow

import (
	"errors"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"

	"regexp"
)

// InboundIssueRow represents a row of the table inboundIssueTable which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "fk_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
type InboundIssueRow struct {
	Timestamp                    string `json:"timestamp"`
	PoNumber                     string `json:"po_number"`
	OrderNumber                  string `json:"order_number"`
	ItemIssueInboundFailedReason string `json:"item_issue_inbound_failed_reason"`
	OrderCancelledYesNo          string `json:"order_cancelled_yes_no"`
	EmailAddress                 string `json:"email_address"`
	OriginalSellerFoundYesNo     string `json:"original_seller_found_yes_no"`
	SupplierName                 string `json:"supplier_name"`
	CategoryDirty                string `json:"category_dirty"`
	BrandDirty                   string `json:"brand_dirty"`
	Sku                          string `json:"sku"`
	StartTimeTroubleshoot        string `json:"start_time_troubleshoot"`
	EndTimeTroubleshoot          string `json:"end_time_troubleshoot"`
	NumberOfItem                 int    `json:"number_of_item"`
	FKSupplier                   string `json:"fk_supplier"`
	IDInboundIssue               string `json:"id_inbound_issue"`
	DurationTroubleshoot         int    `json:"duration_troubleshoot"`
	FKEmail                      string `json:"fk_email"`
	Err                          string `json:"error"`
}

// define validation for each field of InboundIssueRow
func (row InboundIssueRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		// Timestamp cannot be empty, and must be a date in format 2006/01/02 15:04:05
		validation.Field(&row.Timestamp, validation.Required, validation.Date("1/2/2006 15:04:05")),
		validation.Field(&row.PoNumber, validation.By(checkPoNumber)),
		validation.Field(&row.OrderNumber, is.Int, validation.Length(9, 9)),
		// ItemIssueInboundFailedReason cannot be empty and must be within the list specified
		validation.Field(&row.ItemIssueInboundFailedReason, validation.Required, validation.In(
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
		validation.Field(&row.OrderCancelledYesNo, validation.In("Yes", "No")),
		// EmailAddress cannot be empty, and must be an email
		validation.Field(&row.EmailAddress, validation.Required, is.Email),
		validation.Field(&row.OriginalSellerFoundYesNo, validation.In("Yes", "No")),
		validation.Field(&row.Sku, validation.By(checkSku), validation.Length(0, 30)),
		validation.Field(&row.StartTimeTroubleshoot, validation.Date("3:04:05 PM")),
		validation.Field(&row.EndTimeTroubleshoot, validation.Date("3:04:05 PM")),
		validation.Field(&row.NumberOfItem, validation.Required, validation.Min(1)),
		// FKSupplier must be an integer (can be empty if original seller not found)
		validation.Field(&row.FKSupplier, is.Int),
		// FKEmail cannot be empty, and must be an integer
		validation.Field(&row.FKEmail, validation.Required, is.Int),
		validation.Field(&row.DurationTroubleshoot, validation.Min(0)),
	)
}

// FilterInboundIssueTable splits InboundIssueTable into InboundIssueTableValidRow and InboundIssueTableInvalidRow
// NB: rows with FKSupplier = "" & OriginalSellerFoundYesNo = "No" are not considered
func FilterInboundIssueTable(inboundIssueTable []InboundIssueRow) (InboundIssueTableValidRow, InboundIssueTableInvalidRow []InboundIssueRow) {

	isInvalidFormat := filterPointer(inboundIssueTable, isInvalidRowFormat) // incorrect --> stop
	isValidFormat := filterPointer(inboundIssueTable, isValidRowFormat)     // potentially correct --> next filter

	andShouldHaveSupplierName := filter(isValidFormat, isYesOriginalSellerFound) // potentially correct --> next filter
	andDoesNotNeedSupplierName := filter(isValidFormat, isNoOriginalSellerFound) // correct --> stop

	andDoesNotHaveSupplierName := filter(andShouldHaveSupplierName, hasNoSupplierID) // incorrect --> stop
	andHasSupplierName := filter(andShouldHaveSupplierName, hasSupplierID)           // correct --> stop

	// append all correct tables
	InboundIssueTableValidRow = append(andDoesNotNeedSupplierName, andHasSupplierName...)

	// add error message to isInvalidFormat
	for i := 0; i < len(isInvalidFormat); i++ {

		isInvalidFormat[i].Err = isInvalidFormat[i].validateRowFormat().Error()
	}

	// add error message to InboundIssueTableWrongSupplierName
	for i := 0; i < len(andDoesNotHaveSupplierName); i++ {

		andDoesNotHaveSupplierName[i].Err = "Wrong or missing supplier name!"
	}

	// append all incorrect tables
	InboundIssueTableInvalidRow = append(isInvalidFormat, andDoesNotHaveSupplierName...)

	return InboundIssueTableValidRow, InboundIssueTableInvalidRow

}

// filter an array of InboundIssueRow without pointer
func filter(unfilteredTable []InboundIssueRow, test func(InboundIssueRow) bool) (filteredTable []InboundIssueRow) {
	for _, row := range unfilteredTable {
		if test(row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// filter an array of InboundIssueRow with pointer
func filterPointer(unfilteredTable []InboundIssueRow, test func(*InboundIssueRow) bool) (filteredTable []InboundIssueRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if InboundIssueRow has an FKSupplier
func hasSupplierID(row InboundIssueRow) bool {

	return row.FKSupplier != ""

}

// check if InboundIssueRow does not have an FKSupplier
func hasNoSupplierID(row InboundIssueRow) bool {

	return row.FKSupplier == ""

}

// check if InboundIssueRow has OriginalSellerFoundYesNo == "Yes"
func isYesOriginalSellerFound(row InboundIssueRow) bool {

	return row.OriginalSellerFoundYesNo == "Yes"

}

// check if InboundIssueRow has OriginalSellerFoundYesNo == "No"
func isNoOriginalSellerFound(row InboundIssueRow) bool {

	return row.OriginalSellerFoundYesNo == "No"

}

// check if InboundIssueRow has valid format
func isValidRowFormat(row *InboundIssueRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if InboundIssueRow has invalid format
func isInvalidRowFormat(row *InboundIssueRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}

// define data validation for InboundIssueRow.PoNumber
func checkPoNumber(value interface{}) error {
	s, _ := value.(string)
	isMPCD1, _ := regexp.MatchString(`^MPCD-M[[:digit:]]{15}$`, s)
	isMPCD2, _ := regexp.MatchString(`^MPCD-M[[:digit:]]{11}$`, s)
	isC, _ := regexp.MatchString(`^C-[[:digit:]]{9}$`, s)
	isNull, _ := regexp.MatchString(`^$`, s)
	if isMPCD1 || isMPCD2 || isC || isNull {
		return nil
	}
	return errors.New("format incorrect")
}

// define data validation for InboundIssueRow.Sku
func checkSku(value interface{}) error {
	s, _ := value.(string)
	isValid1, _ := regexp.MatchString(`^[[:digit:]]{5}[[:alnum:]]{14}-[[:digit:]]{7}|[[:digit:]]{6}$`, s)
	isValid2, _ := regexp.MatchString(`^[[:alnum:]]{19}-[[:digit:]]{7}|[[:digit:]]{6}$`, s)
	isNull, _ := regexp.MatchString(`^$`, s)
	if isValid1 || isValid2 || isNull {
		return nil
	}
	return errors.New("format incorrect")
}
