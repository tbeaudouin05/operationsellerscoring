package sellerrejectionrow

import (
	"errors"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"

	"regexp"
)

// SellerRejectionRow represents a row of the table sellerRejectionTable which records:
// "timestamp", "item_issue_inbound_failed_reason", "email_address", "original_seller_found_yes_no" and "fk_supplier" of the issues raised by Inbound Troubleshooting team (Warehouse)
type SellerRejectionRow struct {
	Timestamp                    string `json:"timestamp"`
	ItemUID                      string `json:"item_uid"`
	RsReturnOrderNumber          string `json:"rs_return_order_number"`
	ShippingToSellerDate         string `json:"shipping_to_seller_date"`
	RfcReturnFromCustomerReason  string `json:"rfc_return_from_customer_reason"`
	RtsSellerRejectionReason     string `json:"rts_seller_rejection_reason"`
	RtsSellerRejectionDesc       string `json:"rts_seller_rejection_desc"`
	ItemUnitPrice                string `json:"item_unit_price"`
	SupplierName                 string `json:"supplier_name"`
	CustomerOrderNumber          string `json:"customer_order_number"`
	LocationSection              string `json:"location_section"`
	SellerRejectionApprovedYesNo string `json:"seller_rejection_approved_yes_no"`
	ApprovalRejectionDesc        string `json:"approval_rejection_desc"`
	FKSupplier                   string `json:"fk_supplier"`
	IDSellerRejection            string `json:"id_seller_rejection"`
	Err                          string `json:"error"`
}

// define validation for each field of SellerRejectionRow
func (row SellerRejectionRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.Timestamp, validation.Required, validation.Date("1/2/2006")),
		validation.Field(&row.ItemUID, validation.Required, validation.By(checkItemUID)),
		validation.Field(&row.RsReturnOrderNumber, validation.Required, validation.By(checkRsReturnOrderNumber)),
		validation.Field(&row.ShippingToSellerDate, validation.Required, validation.Date("1/2/2006")),
		validation.Field(&row.RfcReturnFromCustomerReason, validation.Required, validation.In(
			`Meets Return Policy / No specific Reason`,
			`Merchant - Wrong Item`,
			`Merchant - Wrong Description`,
			`Other`,
			`Merchant - Defective`,
			`Merchant - Incomplete Items / Missing Parts`,
		)),
		validation.Field(&row.RtsSellerRejectionReason, validation.Required, validation.In(
			`Damaged Package`,
			`Without Any Specific Reason`,
			`Damaged Item`,
			`Missing Parts/Items`,
			`Wrong Item`,
			`Other`,
			`Missed 30 Days SLA`,
			`Wrong Return Reason`,
			`Missing Part or item`,
			`Not Compliant with return Policy`,
		)),
		validation.Field(&row.RtsSellerRejectionDesc, validation.Length(0, 500)),
		validation.Field(&row.ItemUnitPrice, validation.Required, is.Int, validation.Min(0), validation.Max(1000000000)),
		validation.Field(&row.SupplierName, validation.Required, validation.Length(1, 50)),
		validation.Field(&row.CustomerOrderNumber, is.Int, validation.Length(9, 9)),
		validation.Field(&row.LocationSection, validation.Required, validation.In(
			`Damaged`,
			`Claim Rejected`,
			`Qc-Failed`,
			`Wrong Item`,
			`Damaged Package`,
			`Salable`,
		)),
		validation.Field(&row.SellerRejectionApprovedYesNo, validation.Required, validation.In(`Yes`, `No`)),
		validation.Field(&row.ApprovalRejectionDesc, validation.Length(0, 500)),
		validation.Field(&row.IDSellerRejection, validation.Required, validation.Length(0, 50)),
	)
}

// FilterSellerRejectionTable splits SellerRejectionTable into SellerRejectionTableValidRow and SellerRejectionTableInvalidRow
func FilterSellerRejectionTable(sellerRejectionTable []SellerRejectionRow) (SellerRejectionTableValidRow, SellerRejectionTableInvalidRow []SellerRejectionRow) {

	SellerRejectionTableValidRow = filterPointer(sellerRejectionTable, isValidRowFormat)
	SellerRejectionTableInvalidRow = filterPointer(sellerRejectionTable, isInvalidRowFormat)

	// add error message to SellerRejectionTableInvalidRow
	for i := 0; i < len(SellerRejectionTableInvalidRow); i++ {

		SellerRejectionTableInvalidRow[i].Err = SellerRejectionTableInvalidRow[i].validateRowFormat().Error()
	}

	return SellerRejectionTableValidRow, SellerRejectionTableInvalidRow

}

// filter an array of SellerRejectionRow with pointer
func filterPointer(unfilteredTable []SellerRejectionRow, test func(*SellerRejectionRow) bool) (filteredTable []SellerRejectionRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if SellerRejectionRow has valid format
func isValidRowFormat(row *SellerRejectionRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerRejectionRow has invalid format
func isInvalidRowFormat(row *SellerRejectionRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}

// define data validation for SellerRejectionRow.ItemUID
func checkItemUID(value interface{}) error {
	s, _ := value.(string)
	isValid, _ := regexp.MatchString(`^BMLO[[:alnum:]]{8}|[[:alnum:]]{9}$`, s)
	if isValid {
		return nil
	}
	return errors.New("format incorrect")
}

// define data validation for SellerRejectionRow.RsReturnOrderNumber
func checkRsReturnOrderNumber(value interface{}) error {
	s, _ := value.(string)
	isValid, _ := regexp.MatchString(`^RS[[:digit:]]{10}|[[:digit:]]{11}$`, s)
	if isValid {
		return nil
	}
	return errors.New("format incorrect")
}
