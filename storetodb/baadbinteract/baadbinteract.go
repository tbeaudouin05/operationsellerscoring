package baadbinteract

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thomas-bamilo/operationsellerscoring/inboundissuerow"
	"github.com/thomas-bamilo/operationsellerscoring/supplierscorerow"
)

// LoadInboundIssueTableValidRowToBaaDb loads InboundIssueTableValidRow to baa database
func LoadInboundIssueTableValidRowToBaaDb(db *sql.DB, inboundIssueTableValidRow []inboundissuerow.InboundIssueRow) {

	// prepare statement to insert values into inbound_issue table
	insertInboundIssueTableStr := `INSERT INTO baa_application.baa_application_schema.inbound_issue (
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
		log.Println("row number " + string(i))
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
	file, err := os.OpenFile("csvErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	checkError(err)
	defer file.Close()

	// save csvErrorLog to csv
	err = gocsv.MarshalFile(&csvErrorLogP, file)

}

// GetIDInboundFromBaa gets all the existing id_inbound_issue from baa_application.baa_application_schema.inbound_issue and store then into an array of inboundissuerow.InboundIssueRow
func GetIDInboundFromBaa(db *sql.DB) []inboundissuerow.InboundIssueRow {

	// store iDInboundQuery in a string
	iDInboundQuery := `SELECT ii.id_inbound_issue 
	FROM baa_application.baa_application_schema.inbound_issue ii`

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
	,'Wrong Item') 
	THEN 2
	WHEN ii.item_issue_inbound_failed_reason IN (
	'Defective Item'
	,'Defective/Wrong Invoice'
	,'No Invoice')
	THEN 3
	ELSE 0 END) / CAST(COUNT(ii.item_issue_inbound_failed_reason) AS FLOAT) 'inbound_score'
	 
	FROM  baa_application.baa_application_schema.inbound_issue ii
  
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

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
