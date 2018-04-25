package sellerdisciplinerow

import (
	"errors"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"

	"regexp"
)

// SellerDisciplineRow represents a row of the table sellerDisciplineTable which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "id_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
type SellerDisciplineRow struct {
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
	EndTimeTroubleshoot          string `json:"endi_time_troubleshoot"`
	IDSupplier                   string `json:"id_supplier"`
	IDInboundIssue               string `json:"id_inbound_issue"`
	DurationTroubleshoot         int    `json:"duration_troubleshoot"`
	Err                          string `json:"error"`
}

func (row SellerDisciplineRow) validateRowFormat() error {
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
		validation.Field(&row.Sku, validation.By(checkSku)),
		validation.Field(&row.StartTimeTroubleshoot, validation.Date("3:04:05 PM")),
		validation.Field(&row.EndTimeTroubleshoot, validation.Date("3:04:05 PM")),
		// IDSupplier cannot be empty, and must be an integer
		validation.Field(&row.IDSupplier, is.Int),
		validation.Field(&row.DurationTroubleshoot, validation.Min(0)),
	)
}

// FilterSellerDisciplineTable splits SellerDisciplineTable into SellerDisciplineTableWSupplierID and SellerDisciplineTableWrongSupplierName.
// NB: rows with IDSupplier = "" & OriginalSellerFoundYesNo = "No" are not considered
func FilterSellerDisciplineTable(sellerDisciplineTable []SellerDisciplineRow) (SellerDisciplineTableValidRow, SellerDisciplineTableInvalidRow []SellerDisciplineRow) {

	isInvalidFormat := filterPointer(sellerDisciplineTable, isInvalidRowFormat) // incorrect --> stop
	isValidFormat := filterPointer(sellerDisciplineTable, isValidRowFormat)     // potentially correct --> next filter

	andShouldHaveSupplierName := filter(isValidFormat, isYesOriginalSellerFound) // potentially correct --> next filter
	andDoesNotNeedSupplierName := filter(isValidFormat, isNoOriginalSellerFound) // correct --> stop

	andDoesNotHaveSupplierName := filter(andShouldHaveSupplierName, hasNoSupplierID) // incorrect --> stop
	andHasSupplierName := filter(andShouldHaveSupplierName, hasSupplierID)           // correct --> stop

	// append all correct tables
	SellerDisciplineTableValidRow = append(andDoesNotNeedSupplierName, andHasSupplierName...)

	// add error message to isInvalidFormat
	for i := 0; i < len(isInvalidFormat); i++ {

		isInvalidFormat[i].Err = isInvalidFormat[i].validateRowFormat().Error()
	}

	// add error message to SellerDisciplineTableWrongSupplierName
	for i := 0; i < len(andDoesNotHaveSupplierName); i++ {

		andDoesNotHaveSupplierName[i].Err = "Wrong or missing supplier name!"
	}

	// append all incorrect tables
	SellerDisciplineTableInvalidRow = append(isInvalidFormat, andDoesNotHaveSupplierName...)

	return SellerDisciplineTableValidRow, SellerDisciplineTableInvalidRow

}

// filter an array of SellerDisciplineRow without pointer
func filter(unfilteredTable []SellerDisciplineRow, test func(SellerDisciplineRow) bool) (filteredTable []SellerDisciplineRow) {
	for _, row := range unfilteredTable {
		if test(row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// filter an array of SellerDisciplineRow with pointer
func filterPointer(unfilteredTable []SellerDisciplineRow, test func(*SellerDisciplineRow) bool) (filteredTable []SellerDisciplineRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if SellerDisciplineRow has an IDSupplier
func hasSupplierID(row SellerDisciplineRow) bool {

	return row.IDSupplier != ""

}

// check if SellerDisciplineRow does not have an IDSupplier
func hasNoSupplierID(row SellerDisciplineRow) bool {

	return row.IDSupplier == ""

}

// check if SellerDisciplineRow has OriginalSellerFoundYesNo == "Yes"
func isYesOriginalSellerFound(row SellerDisciplineRow) bool {

	return row.OriginalSellerFoundYesNo == "Yes"

}

// check if SellerDisciplineRow has OriginalSellerFoundYesNo == "No"
func isNoOriginalSellerFound(row SellerDisciplineRow) bool {

	return row.OriginalSellerFoundYesNo == "No"

}

// check if SellerDisciplineRow has valid format
func isValidRowFormat(row *SellerDisciplineRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerDisciplineRow has invalid format
func isInvalidRowFormat(row *SellerDisciplineRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}

// define data validation for SellerDisciplineRow.PoNumber
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

// define data validation for SellerDisciplineRow.Sku
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
