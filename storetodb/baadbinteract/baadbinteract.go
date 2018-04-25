package baadbinteract

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/thomas-bamilo/operationsellerscoring/sellerdisciplinerow"
	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/thomas-bamilo/dbconf"
)

// LoadSellerDisciplineTableValidRowToBaaDb loads SellerDisciplineTableValidRow to baa database
func LoadSellerDisciplineTableValidRowToBaaDb(sellerDisciplineTableValidRow []sellerdisciplinerow.SellerDisciplineRow) {

	// fetch database configuration
	var dbConf dbconf.DbConf
	dbConf.ReadYamlDbConf()
	// create connection string
	connStr := `sqlserver://` + dbConf.BaaUser + ":" + dbConf.BaaPw + "@" + dbConf.BaaHost + "/" + dbConf.BaaDb

	// connect to database
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// test connection with ping
	err = db.Ping()
	if err != nil {
		log.Println("Connection failed")
		log.Fatal(err)
	} else {
		log.Println("Connection successful!")
	}

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
		,id_supplier) 
	VALUES (@p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13,@p14,@p15,@p16)`
	insertInboundIssueTable, err := db.Prepare(insertInboundIssueTableStr)
	if err != nil {
		log.Fatal(err)
	}

	// to write csvErrorLog to csv
	file, err := os.OpenFile("csvErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	csvErrorLogP := []*sellerdisciplinerow.SellerDisciplineRow{}

	// write sellerDisciplineTableValidRow into inbound_issue table
	// and write csvErrorLog to csvErrorLog.csv
	// csvErrorLog should only contain PRIMARY KEY constraint messages: rows already uploaded should not be uploaded again
	for i := 0; i < len(sellerDisciplineTableValidRow); i++ {
		_, err = insertInboundIssueTable.Exec(
			sellerDisciplineTableValidRow[i].IDInboundIssue,
			sellerDisciplineTableValidRow[i].Timestamp,
			sellerDisciplineTableValidRow[i].PoNumber,
			sellerDisciplineTableValidRow[i].OrderNumber,
			sellerDisciplineTableValidRow[i].ItemIssueInboundFailedReason,
			sellerDisciplineTableValidRow[i].OrderCancelledYesNo,
			sellerDisciplineTableValidRow[i].EmailAddress,
			sellerDisciplineTableValidRow[i].OriginalSellerFoundYesNo,
			sellerDisciplineTableValidRow[i].SupplierName,
			sellerDisciplineTableValidRow[i].CategoryDirty,
			sellerDisciplineTableValidRow[i].BrandDirty,
			sellerDisciplineTableValidRow[i].Sku,
			sellerDisciplineTableValidRow[i].StartTimeTroubleshoot,
			sellerDisciplineTableValidRow[i].EndTimeTroubleshoot,
			sellerDisciplineTableValidRow[i].NumberOfItem,
			sellerDisciplineTableValidRow[i].IDSupplier,
		)
		if err != nil {
			csvErrorLogP = append(csvErrorLogP,
				&sellerdisciplinerow.SellerDisciplineRow{
					Err:                          string(err.Error()),
					IDInboundIssue:               sellerDisciplineTableValidRow[i].IDInboundIssue,
					Timestamp:                    sellerDisciplineTableValidRow[i].Timestamp,
					PoNumber:                     sellerDisciplineTableValidRow[i].PoNumber,
					OrderNumber:                  sellerDisciplineTableValidRow[i].OrderNumber,
					ItemIssueInboundFailedReason: sellerDisciplineTableValidRow[i].ItemIssueInboundFailedReason,
					OrderCancelledYesNo:          sellerDisciplineTableValidRow[i].OrderCancelledYesNo,
					EmailAddress:                 sellerDisciplineTableValidRow[i].EmailAddress,
					OriginalSellerFoundYesNo:     sellerDisciplineTableValidRow[i].OriginalSellerFoundYesNo,
					SupplierName:                 sellerDisciplineTableValidRow[i].SupplierName,
					CategoryDirty:                sellerDisciplineTableValidRow[i].CategoryDirty,
					BrandDirty:                   sellerDisciplineTableValidRow[i].BrandDirty,
					Sku:                          sellerDisciplineTableValidRow[i].Sku,
					StartTimeTroubleshoot: sellerDisciplineTableValidRow[i].StartTimeTroubleshoot,
					EndTimeTroubleshoot:   sellerDisciplineTableValidRow[i].EndTimeTroubleshoot,
					NumberOfItem:          sellerDisciplineTableValidRow[i].NumberOfItem,
					IDSupplier:            sellerDisciplineTableValidRow[i].IDSupplier})
		}
		time.Sleep(1 * time.Millisecond)
	}

	// save csvErrorLog to csv
	err = gocsv.MarshalFile(&csvErrorLogP, file)

}
