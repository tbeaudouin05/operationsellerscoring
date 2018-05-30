package baadbinteract

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thomas-bamilo/operation/operationsellerscoring/inboundissuerow"
	"github.com/thomas-bamilo/operation/operationsellerscoring/sellerrejectionrow"
	"github.com/thomas-bamilo/operation/operationsellerscoring/supplierscorerow"
)

// LoadInboundIssueTableValidRowToBaaDb loads InboundIssueTableValidRow to baa database
func LoadInboundIssueTableValidRowToBaaDb(db *sql.DB, inboundIssueTableValidRow []inboundissuerow.InboundIssueRow) {

	// prepare statement to insert values into inbound_issue table
	insertInboundIssueTableStr := `INSERT INTO baa_application.operation.inbound_issue (
		id_inbound_issue
		,timestamp 
		,po_number 
		,order_number 
		,item_issue_inbound_failed_reason 
		,order_cancelled_yes_no 
		,email_address 
		,original_seller_found_yes_no
		,supplier_name 
		,category_dirty
		,brand_dirty 
		,sku 
		,start_time_troubleshoot
		,end_time_troubleshoot 
		,number_of_item
		,fk_supplier) 
	VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13,@p14,@p15,@p16)`
	insertInboundIssueTable, err := db.Prepare(insertInboundIssueTableStr)
	checkError(err)

	csvErrorLogP := []*inboundissuerow.InboundIssueRow{}

	// write inboundIssueTableValidRow into inbound_issue table
	// and write csvErrorLog to csvErrorLog.csv
	// csvErrorLog should not contain any data - all data validation should already been taken care of by the application
	for i := 0; i < len(inboundIssueTableValidRow); i++ {
		_, err = insertInboundIssueTable.Exec(
			inboundIssueTableValidRow[i].IDInboundIssue,
			inboundIssueTableValidRow[i].Timestamp,
			inboundIssueTableValidRow[i].PoNumber,
			inboundIssueTableValidRow[i].OrderNumber,
			inboundIssueTableValidRow[i].ItemIssueInboundFailedReason,
			inboundIssueTableValidRow[i].OrderCancelledYesNo,
			inboundIssueTableValidRow[i].EmailAddress,
			inboundIssueTableValidRow[i].OriginalSellerFoundYesNo,
			inboundIssueTableValidRow[i].SupplierName,
			inboundIssueTableValidRow[i].CategoryDirty,
			inboundIssueTableValidRow[i].BrandDirty,
			inboundIssueTableValidRow[i].Sku,
			inboundIssueTableValidRow[i].StartTimeTroubleshoot,
			inboundIssueTableValidRow[i].EndTimeTroubleshoot,
			inboundIssueTableValidRow[i].NumberOfItem,
			inboundIssueTableValidRow[i].FKSupplier,
		)
		if err != nil {
			csvErrorLogP = append(csvErrorLogP,
				&inboundissuerow.InboundIssueRow{
					Err:                          string(err.Error()),
					IDInboundIssue:               inboundIssueTableValidRow[i].IDInboundIssue,
					Timestamp:                    inboundIssueTableValidRow[i].Timestamp,
					PoNumber:                     inboundIssueTableValidRow[i].PoNumber,
					OrderNumber:                  inboundIssueTableValidRow[i].OrderNumber,
					ItemIssueInboundFailedReason: inboundIssueTableValidRow[i].ItemIssueInboundFailedReason,
					OrderCancelledYesNo:          inboundIssueTableValidRow[i].OrderCancelledYesNo,
					EmailAddress:                 inboundIssueTableValidRow[i].EmailAddress,
					OriginalSellerFoundYesNo:     inboundIssueTableValidRow[i].OriginalSellerFoundYesNo,
					SupplierName:                 inboundIssueTableValidRow[i].SupplierName,
					CategoryDirty:                inboundIssueTableValidRow[i].CategoryDirty,
					BrandDirty:                   inboundIssueTableValidRow[i].BrandDirty,
					Sku:                          inboundIssueTableValidRow[i].Sku,
					StartTimeTroubleshoot: inboundIssueTableValidRow[i].StartTimeTroubleshoot,
					EndTimeTroubleshoot:   inboundIssueTableValidRow[i].EndTimeTroubleshoot,
					NumberOfItem:          inboundIssueTableValidRow[i].NumberOfItem,
					FKSupplier:            inboundIssueTableValidRow[i].FKSupplier})
		}
		time.Sleep(1 * time.Millisecond)
	}

	// to write csvErrorLog to csv
	file, err := os.OpenFile("InboundIssueErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	checkError(err)
	defer file.Close()

	// save csvErrorLog to csv
	err = gocsv.MarshalFile(&csvErrorLogP, file)

}

// LoadSellerRejectionTableValidRowToBaaDb loads SellerRejectionTableValidRow to baa database
func LoadSellerRejectionTableValidRowToBaaDb(db *sql.DB, sellerRejectionTableValidRow []sellerrejectionrow.SellerRejectionRow) {

	// prepare statement to insert values into seller_rejection table
	insertSellerRejectionTableStr := `INSERT INTO baa_application.operation.seller_rejection (
		id_seller_rejection
		,timestamp 
		,item_uid
		,rs_return_order_number
		,shipping_to_seller_date
		,rfc_return_from_customer_reason
		,rts_seller_rejection_reason
		,rts_seller_rejection_desc
		,item_unit_price
		,supplier_name
		,customer_order_number
		,location_section
		,seller_rejection_approved_yes_no
		,approval_rejection_desc
		,fk_supplier) 
	VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13,@p14,@p15)`
	insertSellerRejectionTable, err := db.Prepare(insertSellerRejectionTableStr)
	checkError(err)

	csvErrorLogP := []*sellerrejectionrow.SellerRejectionRow{}

	// write sellerRejectionTableValidRow into seller_rejection table
	// and write csvErrorLog to csvErrorLog.csv
	// csvErrorLog should not contain any data - all data validation should already been taken care of by the application
	for i := 0; i < len(sellerRejectionTableValidRow); i++ {
		log.Println("row number " + string(i))
		_, err = insertSellerRejectionTable.Exec(
			sellerRejectionTableValidRow[i].IDSellerRejection,
			sellerRejectionTableValidRow[i].Timestamp,
			sellerRejectionTableValidRow[i].ItemUID,
			sellerRejectionTableValidRow[i].RsReturnOrderNumber,
			sellerRejectionTableValidRow[i].ShippingToSellerDate,
			sellerRejectionTableValidRow[i].RfcReturnFromCustomerReason,
			sellerRejectionTableValidRow[i].RtsSellerRejectionReason,
			sellerRejectionTableValidRow[i].RtsSellerRejectionDesc,
			sellerRejectionTableValidRow[i].ItemUnitPrice,
			sellerRejectionTableValidRow[i].SupplierName,
			sellerRejectionTableValidRow[i].CustomerOrderNumber,
			sellerRejectionTableValidRow[i].LocationSection,
			sellerRejectionTableValidRow[i].SellerRejectionApprovedYesNo,
			sellerRejectionTableValidRow[i].ApprovalRejectionDesc,
			sellerRejectionTableValidRow[i].FKSupplier,
		)
		if err != nil {
			csvErrorLogP = append(csvErrorLogP,
				&sellerrejectionrow.SellerRejectionRow{
					Err:                          string(err.Error()),
					IDSellerRejection:            sellerRejectionTableValidRow[i].IDSellerRejection,
					Timestamp:                    sellerRejectionTableValidRow[i].Timestamp,
					ItemUID:                      sellerRejectionTableValidRow[i].ItemUID,
					RsReturnOrderNumber:          sellerRejectionTableValidRow[i].RsReturnOrderNumber,
					ShippingToSellerDate:         sellerRejectionTableValidRow[i].ShippingToSellerDate,
					RfcReturnFromCustomerReason:  sellerRejectionTableValidRow[i].RfcReturnFromCustomerReason,
					RtsSellerRejectionReason:     sellerRejectionTableValidRow[i].RtsSellerRejectionReason,
					RtsSellerRejectionDesc:       sellerRejectionTableValidRow[i].RtsSellerRejectionDesc,
					ItemUnitPrice:                sellerRejectionTableValidRow[i].ItemUnitPrice,
					SupplierName:                 sellerRejectionTableValidRow[i].SupplierName,
					CustomerOrderNumber:          sellerRejectionTableValidRow[i].CustomerOrderNumber,
					LocationSection:              sellerRejectionTableValidRow[i].LocationSection,
					SellerRejectionApprovedYesNo: sellerRejectionTableValidRow[i].SellerRejectionApprovedYesNo,
					ApprovalRejectionDesc:        sellerRejectionTableValidRow[i].ApprovalRejectionDesc,
					FKSupplier:                   sellerRejectionTableValidRow[i].FKSupplier})
		}
		time.Sleep(1 * time.Millisecond)
	}

	// to write csvErrorLog to csv
	file, err := os.OpenFile("SellerRejectionErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	checkError(err)
	defer file.Close()

	// save csvErrorLog to csv
	err = gocsv.MarshalFile(&csvErrorLogP, file)

}

// GetIDInboundIssueFromBaa gets all the existing id_inbound_issue from baa_application.operation.inbound_issue and store then into an array of inboundissuerow.InboundIssueRow
func GetIDInboundIssueFromBaa(db *sql.DB) []inboundissuerow.InboundIssueRow {

	// store iDInboundQuery in a string
	iDInboundQuery := `SELECT ii.id_inbound_issue 
	FROM baa_application.operation.inbound_issue ii`

	// write iDInboundQuery result to an array of inboundissuerow.InboundIssueRow, this array of rows represents iDInboundIssueTable
	var iDInboundIssue string
	var iDInboundIssueTable []inboundissuerow.InboundIssueRow

	rows, _ := db.Query(iDInboundQuery)

	for rows.Next() {
		err := rows.Scan(&iDInboundIssue)
		checkError(err)
		iDInboundIssueTable = append(iDInboundIssueTable,
			inboundissuerow.InboundIssueRow{
				IDInboundIssue: iDInboundIssue})
	}

	return iDInboundIssueTable
}

// GetIDSellerRejectionFromBaa gets all the existing id_seller_rejection from baa_application.operation.seller_rejection and store then into an array of sellerrejectionrow.SellerRejectionRow
func GetIDSellerRejectionFromBaa(db *sql.DB) []sellerrejectionrow.SellerRejectionRow {

	// store iDSellerRejectionQuery in a string
	iDSellerRejectionQuery := `SELECT sr.id_seller_rejection 
	FROM baa_application.operation.seller_rejection sr`

	// write iDSellerRejectionQuery result to an array of sellerrejectionrow.SellerRejectionRow, this array of rows represents iDSellerRejectionTable
	var iDSellerRejection string
	var iDSellerRejectionTable []sellerrejectionrow.SellerRejectionRow

	rows, _ := db.Query(iDSellerRejectionQuery)

	for rows.Next() {
		err := rows.Scan(&iDSellerRejection)
		checkError(err)
		iDSellerRejectionTable = append(iDSellerRejectionTable,
			sellerrejectionrow.SellerRejectionRow{
				IDSellerRejection: iDSellerRejection})
	}

	return iDSellerRejectionTable
}

// InboundScoreTableFromBaa creates inboundScoreTable which records:
// id_supplier and inbound_score for each supplier
func InboundScoreTableFromBaa(db *sql.DB) []supplierscorerow.SupplierScoreRow {

	// store inboundScoreTableQuery in a string
	inboundScoreTableQuery := `SELECT 
	CONCAT(
		CASE WHEN MONTH(GETDATE()) = 1 
		THEN YEAR(GETDATE())-1 
		ELSE YEAR(GETDATE()) END, 
		CASE WHEN MONTH(GETDATE()) = 1 
		THEN 12 
		ELSE MONTH(GETDATE())-1 END
		) 'year_month'
	,ii.supplier_name
	,ii.fk_supplier 'id_supplier'
	,SUM(CASE 
	WHEN ii.item_issue_inbound_failed_reason IN (
	'Other')
	THEN 1
	WHEN ii.item_issue_inbound_failed_reason IN (
	'No Packaging'
	,'Bad Packaging'
	,'Extra Items Sent by Seller'
	,'Items Not Sorted'
	,'Wrong Item'
	,'Merchant - Wrong color'
	,'Merchant - wrong item'
	,'Merchant - wrong size'
	,'merchant - wrong content') 
	THEN 2
	WHEN ii.item_issue_inbound_failed_reason IN (
	'Defective Item'
	,'Defective/Wrong Invoice'
	,'No Invoice')
	THEN 3
	ELSE 0 END) / CAST(COUNT(ii.item_issue_inbound_failed_reason) AS FLOAT) 'inbound_score'
	 
	FROM  baa_application.operation.inbound_issue ii
  
	WHERE ii.fk_supplier <> 0
	
	AND ii.item_issue_inbound_failed_reason IN (
	'No Packaging'
	,'Bad Packaging'
	,'Extra Items Sent by Seller'
	,'Items Not Sorted'
	,'Wrong Item'
	,'Defective Item'
	,'Defective/Wrong Invoice'
	,'No Invoice'
	,'Other'
	,'Merchant - Wrong color'
	,'Merchant - wrong item'
	,'Merchant - wrong size'
	,'merchant - wrong content'
	)
	
	AND MONTH(ii.timestamp) = CASE WHEN MONTH(GETDATE()) = 1 THEN 12 ELSE MONTH(GETDATE())-1 END
	  AND YEAR(ii.timestamp) = CASE WHEN MONTH(GETDATE()) = 1 THEN YEAR(GETDATE())-1 ELSE YEAR(GETDATE()) END
  
	GROUP BY ii.fk_supplier, ii.supplier_name`

	// write inboundScoreTableQuery result to an array of inboundissuerow.InboundIssueRow, this array of rows represents inboundScoreTable
	var yearMonth, supplierName, iDSupplier string
	var inboundScore float32
	var inboundScoreTable []supplierscorerow.SupplierScoreRow

	rows, _ := db.Query(inboundScoreTableQuery)

	for rows.Next() {
		err := rows.Scan(&yearMonth, &supplierName, &iDSupplier, &inboundScore)
		checkError(err)
		inboundScoreTable = append(inboundScoreTable,
			supplierscorerow.SupplierScoreRow{
				YearMonth:    yearMonth,
				SupplierName: supplierName,
				IDSupplier:   iDSupplier,
				InboundScore: inboundScore})
	}

	return inboundScoreTable
}

// RtsTableFromBaa creates rtsTable which records:
// id_supplier and rts_score for each supplier
func RtsTableFromBaa(db *sql.DB) []supplierscorerow.SupplierScoreRow {

	// store rtsTableQuery in a string
	rtsTableQuery := `SELECT 
	CONCAT(
		CASE WHEN MONTH(GETDATE()) = 1 
		THEN YEAR(GETDATE())-1 
		ELSE YEAR(GETDATE()) END, 
		CASE WHEN MONTH(GETDATE()) = 1 
		THEN 12 
		ELSE MONTH(GETDATE())-1 END
		) 'year_month'
	,sr.supplier_name
	,sr.fk_supplier 'id_supplier'
	,SUM(CASE 
	WHEN sr.rts_seller_rejection_reason IN (
	'Missing Part or item'
  ,'Missing Parts/Items'
  ,'Without Any Specific Reason'
  ,'Wrong Return Reason'
)
	THEN 1
	WHEN sr.rts_seller_rejection_reason IN (
  'Damaged Item'
  ,'Damaged Package'
  ,'Missed 30 Days SLA'
  ,'Not Compliant with return Policy'
) 
	THEN 2
	WHEN sr.rts_seller_rejection_reason IN (
  'Wrong Item'
  ,'Other'
)
	THEN 3
	ELSE 0 END) / CAST(COUNT(sr.rts_seller_rejection_reason) AS FLOAT) 'rts_score'
	 
	FROM  baa_application.operation.seller_rejection sr
  
	WHERE sr.fk_supplier <> 0
	
	AND sr.rts_seller_rejection_reason IN (
	'Missing Part or item'
  ,'Missing Parts/Items'
  ,'Without Any Specific Reason'
  ,'Wrong Return Reason'
  ,'Not Compliant with return Policy'
  ,'Damaged Item'
  ,'Damaged Package'
  ,'Missed 30 Days SLA'
  ,'Wrong Item'
  ,'Other'
	)
	
	AND MONTH(sr.timestamp) = CASE WHEN MONTH(GETDATE()) = 1 THEN 12 ELSE MONTH(GETDATE())-1 END
	AND YEAR(sr.timestamp) = CASE WHEN MONTH(GETDATE()) = 1 THEN YEAR(GETDATE())-1 ELSE YEAR(GETDATE()) END
  
	GROUP BY sr.fk_supplier, sr.supplier_name`

	// write rtsTableQuery result to an array of inboundissuerow.InboundIssueRow, this array of rows represents rtsTable
	var yearMonth, supplierName, iDSupplier string
	var rtsScore float32
	var rtsTable []supplierscorerow.SupplierScoreRow

	rows, _ := db.Query(rtsTableQuery)

	for rows.Next() {
		err := rows.Scan(&yearMonth, &supplierName, &iDSupplier, &rtsScore)
		checkError(err)
		rtsTable = append(rtsTable,
			supplierscorerow.SupplierScoreRow{
				YearMonth:    yearMonth,
				SupplierName: supplierName,
				IDSupplier:   iDSupplier,
				RtsScore:     rtsScore})
	}

	return rtsTable
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
